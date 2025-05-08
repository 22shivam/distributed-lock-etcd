# distributed_lock.py

import etcd3, uuid, time
from etcd3.events import DeleteEvent

# 1) Create a client (point host/port to your etcd endpoint)
client = etcd3.client(host="127.0.0.1", port=2379)

def acquire_lock(resource: str, ttl: int = 30, timeout: float = None):
    """
    Try to acquire a lock named `resource`.
    - ttl: lease time-to-live in seconds
    - timeout: max seconds to wait before giving up
    Returns (owner_id, lease) if successful, else raises TimeoutError.
    """
    key   = f"/locks/{resource}"
    lease = client.lease(ttl)
    owner = str(uuid.uuid4())
    start = time.time()

    while True:
        # attempt atomic create-if-not-exists
        got_it, _ = client.transaction(
            compare=[client.transactions.create(key) == 0],
            success=[client.transactions.put(key, owner, lease.id)],
            failure=[]
        )
        if got_it:
            return owner, lease

        # check for timeout
        if timeout and (time.time() - start) > timeout:
            lease.revoke()
            raise TimeoutError(f"Timeout: failed to acquire '{resource}' in {timeout}s")

        # wait until current lock is released
        events, cancel = client.watch(key)
        for ev in events:
            if isinstance(ev, DeleteEvent):
                cancel()
                break

def release_lock(resource: str, owner: str, lease):
    """
    Release the lock if you still own it. Raises if ownership doesn’t match.
    """
    key = f"/locks/{resource}"
    val, _ = client.get(key)
    if val and val.decode() == owner:
        lease.revoke()
    else:
        raise RuntimeError("Cannot release lock: ownership mismatch")


# === Example usage & simple tests ===
if __name__ == "__main__":
    # 1) Basic acquire/release
    owner, lease = acquire_lock("demo", ttl=10, timeout=5)
    print("Acquired lock as", owner)
    release_lock("demo", owner, lease)
    print("Released lock\n")

    # 2) Lease expiry test
    owner1, lease1 = acquire_lock("expiry_demo", ttl=3, timeout=2)
    print("Lock held by", owner1, "for ~3s (no refresh)...")
    time.sleep(5)
    owner2, lease2 = acquire_lock("expiry_demo", ttl=3, timeout=2)
    print("After expiry, new owner:", owner2)
    release_lock("expiry_demo", owner2, lease2)
