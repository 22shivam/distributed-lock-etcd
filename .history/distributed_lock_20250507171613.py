# distributed_lock.py

import etcd3
import uuid
import time
import threading
from etcd3.events import DeleteEvent

# Create a client (point host/port to your etcd endpoint)
client = etcd3.client(host="127.0.0.1", port=2381)


def safe_revoke(lease):
    """
    Try to revoke a lease, but ignore if the lease has already expired.
    """
    try:
        lease.revoke()
    except Exception as e:
        if "requested lease not found" not in str(e):
            raise


def _wait_for_deletion(key: str, wait_time: float) -> bool:
    """
    Watch `key` for a DeleteEvent for up to `wait_time` seconds.
    Returns True if we saw a deletion, False on timeout or error.
    """
    events, cancel = client.watch(key)
    timer = threading.Timer(wait_time, cancel)
    timer.start()

    for ev in events:
        if isinstance(ev, DeleteEvent):
            timer.cancel()
            cancel()
            return True

    timer.cancel()
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

    while True:
        elapsed = time.time() - start
        # Global timeout: abort if we've waited too long
        if timeout is not None and elapsed >= timeout:
            raise TimeoutError(f"Timed out after {timeout}s waiting for '{resource}'")

        # Create a fresh lease and owner ID
        lease = client.lease(ttl)
        owner = str(uuid.uuid4())

        # Atomic transaction: succeed only if key doesn't exist
        got_it, _ = client.transaction(
            compare=[client.transactions.create(key) == 0],
            success=[client.transactions.put(key, owner, lease.id)],
            failure=[]
        )
        if got_it:
            return owner, lease

        # Failed to acquire: revoke lease immediately
        safe_revoke(lease)

        # Wait for the key to be deleted
        remaining = (timeout - elapsed) if timeout is not None else None
        saw_delete = _wait_for_deletion(key, remaining or 0)
        if not saw_delete:
            raise TimeoutError(f"Timed out after {timeout}s waiting for '{resource}'")
        # Retry


def release_lock(resource: str, owner: str, lease):
    """
    Release the lock by revoking its lease.
    """
    safe_revoke(lease)


class DistributedLock:
    """
    Context manager for a distributed lock using etcd.

    Parameters:
        resource (str): lock name
        ttl (int): lease time-to-live in seconds
        timeout (float): max seconds to wait for acquisition
        auto_renew (bool): if True, refresh lease in background until release

    Usage:
        try:
            with DistributedLock("resource", ttl=30, timeout=10, auto_renew=True) as lock:
                # critical section; lock.owner & lock.lease available
                do_work()
        except TimeoutError:
            handle_lock_failure()
    """
    def __init__(self, resource: str, ttl: int = 30, timeout: float = None, auto_renew: bool = False):
        self.resource = resource
        self.ttl = ttl
        self.timeout = timeout
        self.auto_renew = auto_renew
        self.owner = None
        self.lease = None
        self._renew_thread = None
        self._stop_event = None

    def __enter__(self):
        # Acquire lock (may raise TimeoutError)
        self.owner, self.lease = acquire_lock(self.resource, self.ttl, self.timeout)

        if self.auto_renew:
            # Start background thread to refresh lease periodically
            self._stop_event = threading.Event()
            def _renew_loop():
                interval = self.ttl / 2.0
                while not self._stop_event.wait(interval):
                    try:
                        self.lease.refresh()
                    except Exception:
                        break
            self._renew_thread = threading.Thread(target=_renew_loop, daemon=True)
            self._renew_thread.start()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Stop auto-renew if running
        if self.auto_renew and self._stop_event:
            self._stop_event.set()
            self._renew_thread.join()

        # Always revoke the lease
        release_lock(self.resource, self.owner, self.lease)


# === Example usage & simple tests ===
if __name__ == "__main__":
    # Basic acquire/release without auto-renew
    try:
        with DistributedLock("demo", ttl=10, timeout=5) as lock:
            print("Acquired lock as", lock.owner)
        print("Released lock\n")
    except TimeoutError as e:
        print(e)

    # Acquire with auto-renew for long task
    try:
        with DistributedLock("long_task", ttl=3, timeout=5, auto_renew=False) as lock:
            print("Lock acquired with auto-renew, owner=", lock.owner)
            time.sleep(5)  # longer than ttl but lease stays alive
            print("sleep continued")
        print("Auto-renewed lock released")
    except TimeoutError as e:
        print(e)
