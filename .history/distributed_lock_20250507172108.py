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
        # etcd returns an error if the lease is not found (already expired)
        if "requested lease not found" not in str(e):
            print("lease not found")


def _wait_for_deletion(key: str, wait_time: float) -> bool:
    """
    Watch `key` for a DeleteEvent for up to `wait_time` seconds.
    Returns True if we saw a deletion, False on timeout or error.
    """
    events, cancel = client.watch(key)
    timer = threading.Timer(wait_time, cancel)  # cancel watch after timeout
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
        # 0) Global timeout: abort if we've waited too long
        elapsed = time.time() - start
        if timeout is not None and elapsed >= timeout:
            raise TimeoutError(f"Timed out after {timeout}s waiting for '{resource}'")

        # 1) Create a fresh lease and owner ID
        lease = client.lease(ttl)
        owner = str(uuid.uuid4())

        # 2) Atomic create-if-not-exists transaction
        got_it, _ = client.transaction(
            compare=[client.transactions.create(key) == 0],
            success=[client.transactions.put(key, owner, lease.id)],
            failure=[]
        )
        if got_it:
            return owner, lease

        # 3) Failed to acquire: revoke lease immediately
        safe_revoke(lease)

        # 4) Wait for the key to be deleted (lock released)
        remaining = (timeout - elapsed) if timeout is not None else None
        saw_delete = _wait_for_deletion(key, remaining or 0)
        if not saw_delete:
            raise TimeoutError(f"Timed out after {timeout}s waiting for '{resource}'")
        # Else retry


def release_lock(resource: str, owner: str, lease):
    """
    Release the lock if ownership matches, else raises RuntimeError.
    """
    key = f"/locks/{resource}"
    val, _ = client.get(key)
    if val is None:
        raise RuntimeError("Cannot release lock: lock does not exist")
    if val.decode() == owner:
        print("releasing lock")
        safe_revoke(lease)
    else:
        raise RuntimeError("Cannot release lock: ownership mismatch")


class DistributedLock:
    """
    Context manager for a distributed lock using etcd.

    Usage:
        try:
            with DistributedLock("resource_name", ttl=30, timeout=10, auto_renew=True) as lock:
                # critical section; lock.owner, lock.lease available
                do_work()
        except TimeoutError:
            # lock acquisition failed
            handle_failure()
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

    def __enter__(self):
        # Attempt to acquire lock; may raise TimeoutError
        try 
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
        # Always attempt release if acquired
        try:
            release_lock(self.resource, self.owner, self.lease)
        except Exception as e:
            print(f"Error releasing lock for {self.resource}: {e}")


# === Example usage & simple tests ===
if __name__ == "__main__":
    # 1) Basic acquire/release with context manager
    with DistributedLock("demo", ttl=10, timeout=5) as lock:
        print("Acquired lock as", lock.owner)
    print("Released lock\n")

    # 2) Lease expiry test: TTL shorter than wait
    with DistributedLock("expiry_demo", ttl=10, timeout=5) as lock1:
        print("Lock held by", lock1.owner, "for ~3s (no refresh)...")
        time.sleep(2)
        with DistributedLock("expiry_demo", ttl=10, timeout=3) as lock2:
            print("After expiry, new owner:", lock2.owner)

    # 3) Auto-renew example
    with DistributedLock("long_demo", ttl=5, timeout=5, auto_renew=True) as lock:
        print("Auto-renewed lock acquired, owner=", lock.owner)
        time.sleep(12)  # longer than TTL, still holds
        print("Releasing after long task")
