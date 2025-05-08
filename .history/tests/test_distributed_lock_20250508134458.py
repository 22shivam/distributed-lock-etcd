import threading
import time
import pytest
import etcd3

from distributed_lock import acquire_lock, release_lock, DistributedLock
import distributed_lock

# point at the same etcd instance your code uses
client = etcd3.client(host="127.0.0.1", port=2381)

@pytest.fixture(autouse=True)
def etcd_cleanup():
    # remove any leftover locks
    client.delete_prefix("/locks/")
    yield
    client.delete_prefix("/locks/")

def test_basic_acquire_release():
    owner, lease = acquire_lock("test1", ttl=5, timeout=1)
    val, _ = client.get("/locks/test1")
    assert val.decode() == owner
    release_lock("test1", owner, lease)
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


def test_exclusive_acquire_context_manager():
    # using two separate DistributedLock instances
    lock1 = DistributedLock("exclusive2", ttl=5, timeout=1)
    lock2 = DistributedLock("exclusive2", ttl=5, timeout=0.5)

    with lock1:
        # first lock held
        assert lock1.is_locked
        # second one must time out on enter
        with pytest.raises(TimeoutError):
            with lock2:
                pass

    # after lock1 context exits, lock2 can succeed
    with lock2 as l2:
        assert l2.is_locked

def test_ttl_expiry_detected_inside_block():
    # TTL=1s, no auto-renew; we sleep past it inside the with
    with DistributedLock("shortlived", ttl=1, timeout=1, auto_renew=False) as lock:
        # right after enter it's held
        assert lock.is_locked
        time.sleep(3)
        # TTL should have expired on the server
        assert not lock.is_locked


def test_nested_with_counts_and_release_once():
    # nested with on same instance should not deadlock
    lock = DistributedLock("nested", ttl=3, timeout=1, auto_renew=False)
    with lock:
        first_owner = lock.owner
        with lock:  # re-entrant
            assert lock.owner == first_owner
            # still one key in etcd
            val, _ = client.get("/locks/nested")
            assert val.decode() == first_owner
        # still held
        assert lock.is_locked
    # after outer exit, key gone
    val, _ = client.get("/locks/nested")
    assert val is None




def test_concurrent_acquire_different_clients():
    # Thread A uses client@2381 and should acquire the lock.
    # Thread B uses client@2379 and should time out while A holds it.

    results = []

    def worker(name, port, hold_time, timeout):
        # Monkey-patch the module-level client to point at the given port
        distributed_lock.client = etcd3.client(host="127.0.0.1", port=port)
        try:
            with distributed_lock.DistributedLock(
                "shared", ttl=5, timeout=timeout, auto_renew=False
            ) as lock:
                results.append((name, "acquired", port))
                time.sleep(hold_time)
        except TimeoutError:
            results.append((name, "timeout", port))

    tA = threading.Thread(target=worker, args=("A", 2381, 2.0, 2.0))
    tB = threading.Thread(target=worker, args=("B", 2379, 0.0, 1.0))

    # Start A first so it grabs the lock
    tA.start()
    time.sleep(0.1)
    tB.start()

    tA.join()
    tB.join()

    # Verify A acquired via port 2381, B timed out on port 2379
    assert ("A", "acquired", 2381) in results
    assert ("B", "timeout", 2379) in results


