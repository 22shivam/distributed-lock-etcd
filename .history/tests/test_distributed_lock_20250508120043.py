# tests/test_distributed_lock.py
import threading
import time
import pytest
import etcd3

from distributed_lock import acquire_lock, release_lock, DistributedLock

# point at the same etcd instance your code uses
client = etcd3.client(host="127.0.0.1", port=2381)

@pytest.fixture(autouse=True)
def etcd_cleanup():
    # remove any leftover locks
    client.delete_prefix("/locks/")
    yield
    client.delete_prefix("/locks/")

def test_basic_acquire_release():
    # acquire
    owner, lease = acquire_lock("test1", ttl=5, timeout=1)
    # key should exist and match owner
    val, _ = client.get("/locks/test1")
    assert val.decode() == owner
    # release
    release_lock("test1", owner, lease)
    # key should be gone
    val, _ = client.get("/locks/test1")
    assert val is None

def test_lease_expiry_allows_reacquire():
    # short TTL, then sleep past it
    owner1, lease1 = acquire_lock("test2", ttl=1, timeout=1)
    time.sleep(2)
    owner2, lease2 = acquire_lock("test2", ttl=1, timeout=1)
    assert owner2 != owner1
    release_lock("test2", owner2, lease2)

def test_reentrant_behavior():
    with DistributedLock("test3", ttl=5, timeout=1) as lock:
        first = lock.owner
        # nested re-entry should not block and owner stays same
        with lock:
            assert lock.owner == first
    # after outer exit, lock is gone
    val, _ = client.get("/locks/test3")
    assert val is None

def test_auto_renew_holds_longer_than_ttl():
    # start time
    t0 = time.time()
    with DistributedLock("test4", ttl=1, timeout=1, auto_renew=True) as lock:
        time.sleep(3)  # > ttl, but auto-renew should keep it alive
        # verify it's still held
        val, _ = client.get("/locks/test4")
        assert val.decode() == lock.owner
    # after exit it's released
    val, _ = client.get("/locks/test4")
    assert val is None
    assert time.time() - t0 >= 3

def test_concurrent_acquire_times_out_one_and_succeeds_other():
    results = []

    def worker(name, sleep_before_release, timeout):
        try:
            with DistributedLock("test5", ttl=5, timeout=timeout) as lock:
                results.append((name, "acquired", lock.owner))
                time.sleep(sleep_before_release)
        except TimeoutError:
            results.append((name, "timeout", None))

    # First thread holds lock for 2s, second only waits 1s
    t1 = threading.Thread(target=worker, args=("t1", 2, 3))
    t2 = threading.Thread(target=worker, args=("t2", 0, 1))
    t1.start()
    time.sleep(0.1)  # ensure t1 acquires first
    t2.start()
    t1.join()
    t2.join()

    # One acquired first, second times out
    assert ("t1", "acquired",) in [(r[0], r[1]) for r in results]
    assert ("t2", "timeout",) in [(r[0], r[1]) for r in results]


def test_lock_is_locked():
    with DistributedLock("test6", ttl=5, timeout=1) as lock:
        assert lock.is_locked
    assert not lock.is_locked

def test_exclusive_acquire_direct():
    # 1) one holder succeeds
    owner1, lease1 = acquire_lock("exclusive", ttl=5, timeout=1)
    # 2) direct second acquire must fail
    with pytest.raises(TimeoutError):
        acquire_lock("exclusive", ttl=5, timeout=0.5)
    # 3) after release, a new owner can succeed
    release_lock("exclusive", owner1, lease1)
    owner2, lease2 = acquire_lock("exclusive", ttl=5, timeout=1)
    assert owner2 != owner1
    release_lock("exclusive", owner2, lease2)

def test_exclusive_acquire_indirect():
    # 1) one holder succeeds
    owner1, lease1 = acquire_lock("exclusive", ttl=5, timeout=1)
    # 2) indirect second acquire must fail
    with pytest.raises(TimeoutError):
        with DistributedLock("exclusive", ttl=5, timeout=0.5):
            pass
    # 3) after release, a new owner can succeed
    release_lock("exclusive", owner1, lease1)
    owner2, lease2 = acquire_lock("exclusive", ttl=5, timeout=1)
    assert owner2 != owner1
    release_lock("exclusive", owner2, lease2)



