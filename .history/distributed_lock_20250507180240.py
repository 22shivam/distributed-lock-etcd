# distributed_lock.py

import logging
import etcd3
import uuid
import time
import threading
from etcd3.events import DeleteEvent

# Configure logging
# Configure logging — send to both console and a file
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        #logging.StreamHandler(),                         # console
        logging.FileHandler("distributed_lock.log"),      # file
    ]
)
logger = logging.getLogger(__name__)

# Create a client (point host/port to your etcd endpoint)
client = etcd3.client(host="127.0.0.1", port=2381)


def safe_revoke(lease):
    """
    Try to revoke a lease, but ignore if the lease has already expired.
    """
    try:
        lease.revoke()
        logger.debug("Lease %s revoked", getattr(lease, 'id', None))
    except Exception as e:
        # etcd returns an error if the lease is not found (already expired)
        if "requested lease not found" not in str(e):
            logger.error("Error revoking lease: %s", e)
        else:
            logger.warning("Lease not found (already expired)")


def _wait_for_deletion(key: str, wait_time: float) -> bool:
    """
    Watch `key` for a DeleteEvent for up to `wait_time` seconds.
    Returns True if we saw a deletion, False on timeout or error.
    """
    logger.debug("Watching key %s for deletion up to %.2fs", key, wait_time)
    events, cancel = client.watch(key)
    timer = threading.Timer(wait_time, cancel)
    timer.start()

    for ev in events:
        if isinstance(ev, DeleteEvent):
            logger.debug("DeleteEvent received for key %s", key)
            timer.cancel()
            cancel()
            return True

    timer.cancel()
    logger.debug("Watch for key %s ended without deletion", key)
    return False


def acquire_lock(resource: str, ttl: int = 30, timeout: float = None):
    """
    Try to acquire a lock named `resource`.
    - ttl: lease time-to-live in seconds
    - timeout: max seconds to wait before giving up

    Returns:
        (owner_id: str, lease) if successful,
        else raises TimeoutError.
    """
    key = f"/locks/{resource}"
    start = time.time()
    logger.info("Attempting to acquire lock '%s' (ttl=%ss, timeout=%ss)", resource, ttl, timeout)

    while True:
        elapsed = time.time() - start
        # 0) Global timeout: abort if we've waited too long
        if timeout is not None and elapsed >= timeout:
            logger.error("Timeout acquiring lock '%s' after %.2fs", resource, elapsed)
            raise TimeoutError(f"Timed out after {timeout}s waiting for '{resource}'")

        # 1) Create a fresh lease and owner ID
        lease = client.lease(ttl)
        owner = str(uuid.uuid4())
        logger.debug("Created lease %s for owner %s", lease.id, owner)

        # 2) Atomic create-if-not-exists transaction
        got_it, _ = client.transaction(
            compare=[client.transactions.create(key) == 0],
            success=[client.transactions.put(key, owner, lease.id)],
            failure=[]
        )
        if got_it:
            logger.info("Lock '%s' acquired by %s (lease %s)", resource, owner, lease.id)
            return owner, lease

        # 3) Failed to acquire: revoke lease immediately
        logger.debug("Failed to acquire lock '%s'; revoking lease %s", resource, lease.id)
        safe_revoke(lease)

        # 4) Wait for the key to be deleted (lock released)
        remaining = (timeout - elapsed) if timeout is not None else None
        saw_delete = _wait_for_deletion(key, remaining or 0)
        if not saw_delete:
            logger.error("Timeout waiting for release of lock '%s' after %.2fs", resource, elapsed)
            raise TimeoutError(f"Timed out after {timeout}s waiting for '{resource}'")
        # Else retry


def release_lock(resource: str, owner: str, lease):
    """
    Release the lock if ownership matches, else raises RuntimeError.
    """
    key = f"/locks/{resource}"
    val, _ = client.get(key)
    if val is None:
        logger.warning("Cannot release lock '%s': does not exist", resource)
    elif val.decode() == owner:
        logger.info("Releasing lock '%s' held by %s (lease %s)", resource, owner, lease.id)
        safe_revoke(lease)
    else:
        logger.error("Cannot release lock '%s': ownership mismatch (owner=%s)", resource, owner)
        raise RuntimeError("Cannot release lock: ownership mismatch")


class DistributedLock:
    """
    Re-entrant context manager for a distributed lock using etcd.

    Parameters:
        resource (str): lock name
        ttl (int): lease time-to-live in seconds
        timeout (float): max seconds to wait for acquisition
        auto_renew (bool): if True, refresh lease in background until release

    Usage:
        try:
            with DistributedLock("res", ttl=30, timeout=10, auto_renew=True) as lock:
                # critical section
                pass
        except TimeoutError:
            # lock acquisition failed
            pass
    """
    def __init__(self, resource: str, ttl: int = 30, timeout: float = None, auto_renew: bool = False):
        self.resource = resource
        self.ttl = ttl
        self.timeout = timeout
        self.auto_renew = auto_renew
        self.owner = None
        self.lease = None
        # internal for renew thread
        self._stop_event = None
        self._renew_thread = None
        # re-entrancy count
        self._count = 0

    def __enter__(self):
        # If already held by this instance, just bump count
        if self._count > 0:
            self._count += 1
            logger.debug("Re-entering lock '%s', count=%d", self.resource, self._count)
            return self
        # Attempt to acquire lock; may raise TimeoutError
        self.owner, self.lease = acquire_lock(self.resource, self.ttl, self.timeout)
        self._count = 1
        # Start auto-renewal if enabled
        if self.auto_renew:
            self._stop_event = threading.Event()
            def _renew_loop():
                interval = self.ttl / 2.0
                while not self._stop_event.wait(interval):
                    try:
                        self.lease.refresh()
                        logger.debug("Lease %s refreshed", self.lease.id)
                    except Exception as e:
                        logger.error("Error refreshing lease %s: %s", self.lease.id, e)
                        break
            self._renew_thread = threading.Thread(target=_renew_loop, daemon=True)
            self._renew_thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Nested exits: decrement count and only cleanup on zero
        self._count -= 1
        if self._count > 0:
            logger.debug("Exit called, still holding lock '%s', count=%d", self.resource, self._count)
            return
        # Stop auto-renew if running
        if self.auto_renew and self._stop_event:
            self._stop_event.set()
            self._renew_thread.join()
        # Finally release the lock
        try:
            release_lock(self.resource, self.owner, self.lease)
        except Exception as e:
            logger.error("Error releasing lock for %s: %s", self.resource, e)
    

    @property
    def is_locked(self) -> bool:
        """
        Returns True if the lock key still exists in etcd and is owned by us.
        """
        key = f"/locks/{self.resource}"
        val, _ = client.get(key)
        return val is not None and val.decode() == self.owner


# === Example usage & simple tests ===
if __name__ == "__main__":
    # 1) Basic acquire/release
    # with DistributedLock("demo", ttl=10, timeout=5) as lock:
    #     print("Acquired lock as", lock.owner)
    # print("Released lock\n")

    # # 2) Re-entrant example: nested with
    # with DistributedLock("demo2", ttl=10, timeout=5) as lock:
    #     print("First acquire", lock.owner)
    #     with lock:
    #         print("Re-entered", lock.owner)
    # print("Released after nested\n")

    # # 3) Auto-renew example
    # with DistributedLock("long_demo", ttl=3, timeout=5, auto_renew=True) as lock:
    #     print("Auto-renewed lock acquired, owner=", lock.owner)
    #     time.sleep(7)  # longer than TTL, still holds
    #     print("Releasing after long task")
    
    with DistributedLock("job42", ttl=30, timeout=5, auto_renew=True) as lock:
        print("Lease has", lock.ttl_remaining(), "seconds remaining")
        # … do work …
        if not lock.is_locked:
            raise RuntimeError("Oops, we lost the lock!")
        else:
            print("Lock is still held")
