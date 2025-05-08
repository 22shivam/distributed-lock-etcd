# distributed_lock.py

import etcd3
import uuid
import time
import threading
from etcd3.events import DeleteEvent

# 1) Create a client (point host/port to your etcd endpoint)
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
    # schedule a cancel() after wait_time to enforce a timeout
    timer = threading.Timer(wait_time, cancel)
    timer.start()

    for ev in events:
        if isinstance(ev, DeleteEvent):
            timer.cancel()
            cancel()
            return True

    # Watch ended or timed out
    timer.cancel()
    return False


def acquire_lock(resource: str, ttl: int = 30, timeout: float = None):
    """
    Try to acquire a lock named `resource`.
    - ttl: lease time-to-live in seconds
    - timeout: max seconds to wait before giving up
    Returns (owner_id, lease) if successful, else raises TimeoutError.
    """
    key = f"/locks/{resource}"
    start = time.time()

    while True:
        # 0) Global timeout: abort if we've already waited too long
        elapsed = time.time() - start
        if timeout is not None and elapsed >= timeout:
            raise TimeoutError(f"Timed out after {timeout}s waiting for '{resource}'")

        # 1) Create a new lease and owner
        lease = client.lease(ttl)
        owner = str(uuid.uuid4())

        # 2) Atomic create-if-not-exists
        got_it, _ = client.transaction(
            compare=[client.transactions.create(key) == 0],
            success=[client.transactions.put(key, owner, lease.id)],
            failure=[]
        )
        if got_it:
            # we own the lock now
            return owner, lease

        # 3) Failed to grab it — revoke this lease immediately
        safe_revoke(lease)

        # 4) Wait until the key is deleted or we run out of time
        remaining = (timeout - elapsed) if timeout is not None else None
        saw_delete = _wait_for_deletion(key, remaining or 0)
        if not saw_delete:
            # timed out watching for deletion
            raise TimeoutError(f"Timed out after {timeout}s waiting for '{resource}'")
        # else loop and retry with a fresh lease


def release_lock(resource: str, owner: str, lease):
    """
    Release the lock if you still own it. Raises if ownership doesn't match.
    """
    key = f"/locks/{resource}"
    val, _ = client.get(key)
    if val and val.decode() == owner:
        safe_revoke(lease)
    else:
        raise RuntimeError("Cannot release lock: ownership mismatch")


# === Example usage & simple tests ===
if __name__ == "__main__":
    # 1) Basic acquire/release
    try:
        owner, lease = acquire_lock("demo", ttl=10, timeout=5)
        print("Acquired lock as", owner)
        release_lock("demo", owner, lease)
        print("Released lock\n")
    except TimeoutError as e:
        print(e)
    except Exception as e:
        print(e)

        # 2) Lease expiry test: TTL shorter than sleep to force expiry
    try: 
        owner1, lease1 = acquire_lock("expiry_demo", ttl=10, timeout=5)
        print("Lock held by", owner1, "for ~3s (no refresh)...")
        # time.sleep(5)  # > TTL, so lease1 expires and key is deleted
        owner2, lease2 = acquire_lock("expiry_demo", ttl=3, timeout=2)
        print("After expiry, new owner:", owner2)
        release_lock("expiry_demo", owner2, lease2)
    except TimeoutError as e:
        print(e)
    except Exception as e:
        print(e)
