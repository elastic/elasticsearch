/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package jsr166e.extra;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.AbstractQueuedLongSynchronizer;
import java.util.Collection;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.IOException;

/**
 * A reentrant mutual exclusion {@link Lock} in which each lock
 * acquisition or release advances a sequence number.  When the
 * sequence number (accessible using {@link #getSequence()}) is odd,
 * the lock is held. When it is even (i.e., ({@code lock.getSequence()
 * & 1L) == 0L}), the lock is released. Method {@link
 * #awaitAvailability} can be used to await availability of the lock,
 * returning its current sequence number. Sequence numbers (as well as
 * reentrant hold counts) are of type {@code long} to ensure that they
 * will not wrap around until hundreds of years of use under current
 * processor rates.  A SequenceLock can be created with a specified
 * number of spins. Attempts to acquire the lock in method {@link
 * #lock} will retry at least the given number of times before
 * blocking. If not specified, a default, possibly platform-specific,
 * value is used.
 *
 * <p>Except for the lack of support for specified fairness policies,
 * or {@link Condition} objects, a SequenceLock can be used in the
 * same way as {@link ReentrantLock}. It provides similar status and
 * monitoring methods, such as {@link #isHeldByCurrentThread}.
 * SequenceLocks may be preferable in contexts in which multiple
 * threads invoke short read-only methods much more frequently than
 * fully locked methods.
 *
 * <p>Methods {@code awaitAvailability} and {@code getSequence} can
 * be used together to define (partially) optimistic read-only methods
 * that are usually more efficient than ReadWriteLocks when they
 * apply.  These methods should in general be structured as loops that
 * await lock availability, then read {@code volatile} fields into
 * local variables (and may further read other values derived from
 * these, for example the {@code length} of a {@code volatile} array),
 * and retry if the sequence number changed while doing so.
 * Alternatively, because {@code awaitAvailability} accommodates
 * reentrancy, a method can retry a bounded number of times before
 * switching to locking mode.  While conceptually straightforward,
 * expressing these ideas can be verbose. For example:
 *
 *  <pre> {@code
 * class Point {
 *   private volatile double x, y;
 *   private final SequenceLock sl = new SequenceLock();
 *
 *   // an exclusively locked method
 *   void move(double deltaX, double deltaY) {
 *     sl.lock();
 *     try {
 *       x += deltaX;
 *       y += deltaY;
 *     } finally {
 *       sl.unlock();
 *     }
 *   }
 *
 *   // A read-only method
 *   double distanceFromOriginV1() {
 *     double currentX, currentY;
 *     long seq;
 *     do {
 *       seq = sl.awaitAvailability();
 *       currentX = x;
 *       currentY = y;
 *     } while (sl.getSequence() != seq); // retry if sequence changed
 *     return Math.sqrt(currentX * currentX + currentY * currentY);
 *   }
 *
 *   // Uses bounded retries before locking
 *   double distanceFromOriginV2() {
 *     double currentX, currentY;
 *     long seq;
 *     int retries = RETRIES_BEFORE_LOCKING; // for example 8
 *     try {
 *       do {
 *         if (--retries < 0)
 *           sl.lock();
 *         seq = sl.awaitAvailability();
 *         currentX = x;
 *         currentY = y;
 *       } while (sl.getSequence() != seq);
 *     } finally {
 *       if (retries < 0)
 *         sl.unlock();
 *     }
 *     return Math.sqrt(currentX * currentX + currentY * currentY);
 *   }
 * }}</pre>
 *
 * @since 1.8
 * @author Doug Lea
 */
public class SequenceLock implements Lock, java.io.Serializable {
    private static final long serialVersionUID = 7373984872572414699L;

    static final class Sync extends AbstractQueuedLongSynchronizer {
        private static final long serialVersionUID = 2540673546047039555L;

        /**
         * The number of times to spin in lock() and awaitAvailability().
         */
        final int spins;

        /**
         * The number of reentrant holds on this lock. Uses a long for
         * compatibility with other AbstractQueuedLongSynchronizer
         * operations. Accessed only by lock holder.
         */
        long holds;

        Sync(int spins) { this.spins = spins; }

        // overrides of AQLS methods

        public final boolean isHeldExclusively() {
            return (getState() & 1L) != 0L &&
                getExclusiveOwnerThread() == Thread.currentThread();
        }

        public final boolean tryAcquire(long acquires) {
            Thread current = Thread.currentThread();
            long c = getState();
            if ((c & 1L) == 0L) {
                if (compareAndSetState(c, c + 1L)) {
                    holds = acquires;
                    setExclusiveOwnerThread(current);
                    return true;
                }
            }
            else if (current == getExclusiveOwnerThread()) {
                holds += acquires;
                return true;
            }
            return false;
        }

        public final boolean tryRelease(long releases) {
            if (Thread.currentThread() != getExclusiveOwnerThread())
                throw new IllegalMonitorStateException();
            if ((holds -= releases) == 0L) {
                setExclusiveOwnerThread(null);
                setState(getState() + 1L);
                return true;
            }
            return false;
        }

        public final long tryAcquireShared(long unused) {
            return (((getState() & 1L) == 0L) ? 1L :
                    (getExclusiveOwnerThread() == Thread.currentThread()) ? 0L:
                    -1L);
        }

        public final boolean tryReleaseShared(long unused) {
            return (getState() & 1L) == 0L;
        }

        public final Condition newCondition() {
            throw new UnsupportedOperationException();
        }

        // Other methods in support of SequenceLock

        final long getSequence() {
            return getState();
        }

        final void lock() {
            int k = spins;
            while (!tryAcquire(1L)) {
                if (k == 0) {
                    acquire(1L);
                    break;
                }
                --k;
            }
        }

        final long awaitAvailability() {
            long s;
            while (((s = getState()) & 1L) != 0L &&
                   getExclusiveOwnerThread() != Thread.currentThread()) {
                acquireShared(1L);
                releaseShared(1L);
            }
            return s;
        }

        final long tryAwaitAvailability(long nanos)
            throws InterruptedException, TimeoutException {
            Thread current = Thread.currentThread();
            for (;;) {
                long s = getState();
                if ((s & 1L) == 0L || getExclusiveOwnerThread() == current) {
                    releaseShared(1L);
                    return s;
                }
                if (!tryAcquireSharedNanos(1L, nanos))
                    throw new TimeoutException();
                // since tryAcquireSharedNanos doesn't return seq
                // retry with minimal wait time.
                nanos = 1L;
            }
        }

        final boolean isLocked() {
            return (getState() & 1L) != 0L;
        }

        final Thread getOwner() {
            return (getState() & 1L) == 0L ? null : getExclusiveOwnerThread();
        }

        final long getHoldCount() {
            return isHeldExclusively() ? holds : 0;
        }

        private void readObject(ObjectInputStream s)
            throws IOException, ClassNotFoundException {
            s.defaultReadObject();
            holds = 0L;
            setState(0L); // reset to unlocked state
        }
    }

    private final Sync sync;

    /**
     * The default spin value for constructor. Future versions of this
     * class might choose platform-specific values.  Currently, except
     * on uniprocessors, it is set to a small value that overcomes near
     * misses between releases and acquires.
     */
    static final int DEFAULT_SPINS =
        Runtime.getRuntime().availableProcessors() > 1 ? 64 : 0;

    /**
     * Creates an instance of {@code SequenceLock} with the default
     * number of retry attempts to acquire the lock before blocking.
     */
    public SequenceLock() { sync = new Sync(DEFAULT_SPINS); }

    /**
     * Creates an instance of {@code SequenceLock} that will retry
     * attempts to acquire the lock at least the given number of times
     * before blocking.
     *
     * @param spins the number of times before blocking
     */
    public SequenceLock(int spins) { sync = new Sync(spins); }

    /**
     * Returns the current sequence number of this lock.  The sequence
     * number is advanced upon each acquire or release action. When
     * this value is odd, the lock is held; when even, it is released.
     *
     * @return the current sequence number
     */
    public long getSequence() { return sync.getSequence(); }

    /**
     * Returns the current sequence number when the lock is, or
     * becomes, available. A lock is available if it is either
     * released, or is held by the current thread.  If the lock is not
     * available, the current thread becomes disabled for thread
     * scheduling purposes and lies dormant until the lock has been
     * released by some other thread.
     *
     * @return the current sequence number
     */
    public long awaitAvailability() { return sync.awaitAvailability(); }

    /**
     * Returns the current sequence number if the lock is, or
     * becomes, available within the specified waiting time.
     *
     * <p>If the lock is not available, the current thread becomes
     * disabled for thread scheduling purposes and lies dormant until
     * one of three things happens:
     *
     * <ul>
     *
     * <li>The lock becomes available, in which case the current
     * sequence number is returned.
     *
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread, in which case this method throws
     * {@link InterruptedException}.
     *
     * <li>The specified waiting time elapses, in which case
     * this method throws {@link TimeoutException}.
     *
     * </ul>
     *
     * @param timeout the time to wait for availability
     * @param unit the time unit of the timeout argument
     * @return the current sequence number if the lock is available
     *         upon return from this method
     * @throws InterruptedException if the current thread is interrupted
     * @throws TimeoutException if the lock was not available within
     * the specified waiting time
     * @throws NullPointerException if the time unit is null
     */
    public long tryAwaitAvailability(long timeout, TimeUnit unit)
        throws InterruptedException, TimeoutException {
        return sync.tryAwaitAvailability(unit.toNanos(timeout));
    }

    /**
     * Acquires the lock.
     *
     * <p>If the current thread already holds this lock then the hold count
     * is incremented by one and the method returns immediately without
     * incrementing the sequence number.
     *
     * <p>If this lock not held by another thread, this method
     * increments the sequence number (which thus becomes an odd
     * number), sets the lock hold count to one, and returns
     * immediately.
     *
     * <p>If the lock is held by another thread then the current
     * thread may retry acquiring this lock, depending on the {@code
     * spin} count established in constructor.  If the lock is still
     * not acquired, the current thread becomes disabled for thread
     * scheduling purposes and lies dormant until enabled by
     * some other thread releasing the lock.
     */
    public void lock() { sync.lock(); }

    /**
     * Acquires the lock unless the current thread is
     * {@linkplain Thread#interrupt interrupted}.
     *
     * <p>If the current thread already holds this lock then the hold count
     * is incremented by one and the method returns immediately without
     * incrementing the sequence number.
     *
     * <p>If this lock not held by another thread, this method
     * increments the sequence number (which thus becomes an odd
     * number), sets the lock hold count to one, and returns
     * immediately.
     *
     * <p>If the lock is held by another thread then the current
     * thread may retry acquiring this lock, depending on the {@code
     * spin} count established in constructor.  If the lock is still
     * not acquired, the current thread becomes disabled for thread
     * scheduling purposes and lies dormant until one of two things
     * happens:
     *
     * <ul>
     *
     * <li>The lock is acquired by the current thread; or
     *
     * <li>Some other thread {@linkplain Thread#interrupt interrupts} the
     * current thread.
     *
     * </ul>
     *
     * <p>If the lock is acquired by the current thread then the lock hold
     * count is set to one and the sequence number is incremented.
     *
     * <p>If the current thread:
     *
     * <ul>
     *
     * <li>has its interrupted status set on entry to this method; or
     *
     * <li>is {@linkplain Thread#interrupt interrupted} while acquiring
     * the lock,
     *
     * </ul>
     *
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * <p>In this implementation, as this method is an explicit
     * interruption point, preference is given to responding to the
     * interrupt over normal or reentrant acquisition of the lock.
     *
     * @throws InterruptedException if the current thread is interrupted
     */
    public void lockInterruptibly() throws InterruptedException {
        sync.acquireInterruptibly(1L);
    }

    /**
     * Acquires the lock only if it is not held by another thread at the time
     * of invocation.
     *
     * <p>If the current thread already holds this lock then the hold
     * count is incremented by one and the method returns {@code true}
     * without incrementing the sequence number.
     *
     * <p>If this lock not held by another thread, this method
     * increments the sequence number (which thus becomes an odd
     * number), sets the lock hold count to one, and returns {@code
     * true}.
     *
     * <p>If the lock is held by another thread then this method
     * returns {@code false}.
     *
     * @return {@code true} if the lock was free and was acquired by the
     *         current thread, or the lock was already held by the current
     *         thread; and {@code false} otherwise
     */
    public boolean tryLock() { return sync.tryAcquire(1L); }

    /**
     * Acquires the lock if it is not held by another thread within the given
     * waiting time and the current thread has not been
     * {@linkplain Thread#interrupt interrupted}.
     *
     * <p>If the current thread already holds this lock then the hold count
     * is incremented by one and the method returns immediately without
     * incrementing the sequence number.
     *
     * <p>If this lock not held by another thread, this method
     * increments the sequence number (which thus becomes an odd
     * number), sets the lock hold count to one, and returns
     * immediately.
     *
     * <p>If the lock is held by another thread then the current
     * thread may retry acquiring this lock, depending on the {@code
     * spin} count established in constructor.  If the lock is still
     * not acquired, the current thread becomes disabled for thread
     * scheduling purposes and lies dormant until one of three things
     * happens:
     *
     * <ul>
     *
     * <li>The lock is acquired by the current thread; or
     *
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread; or
     *
     * <li>The specified waiting time elapses
     *
     * </ul>
     *
     * <p>If the lock is acquired then the value {@code true} is returned and
     * the lock hold count is set to one.
     *
     * <p>If the current thread:
     *
     * <ul>
     *
     * <li>has its interrupted status set on entry to this method; or
     *
     * <li>is {@linkplain Thread#interrupt interrupted} while
     * acquiring the lock,
     *
     * </ul>
     * then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * <p>If the specified waiting time elapses then the value {@code false}
     * is returned.  If the time is less than or equal to zero, the method
     * will not wait at all.
     *
     * <p>In this implementation, as this method is an explicit
     * interruption point, preference is given to responding to the
     * interrupt over normal or reentrant acquisition of the lock, and
     * over reporting the elapse of the waiting time.
     *
     * @param timeout the time to wait for the lock
     * @param unit the time unit of the timeout argument
     * @return {@code true} if the lock was free and was acquired by the
     *         current thread, or the lock was already held by the current
     *         thread; and {@code false} if the waiting time elapsed before
     *         the lock could be acquired
     * @throws InterruptedException if the current thread is interrupted
     * @throws NullPointerException if the time unit is null
     */
    public boolean tryLock(long timeout, TimeUnit unit)
        throws InterruptedException {
        return sync.tryAcquireNanos(1L, unit.toNanos(timeout));
    }

    /**
     * Attempts to release this lock.
     *
     * <p>If the current thread is the holder of this lock then the
     * hold count is decremented.  If the hold count is now zero then
     * the sequence number is incremented (thus becoming an even
     * number) and the lock is released.  If the current thread is not
     * the holder of this lock then {@link
     * IllegalMonitorStateException} is thrown.
     *
     * @throws IllegalMonitorStateException if the current thread does not
     *         hold this lock
     */
    public void unlock() { sync.release(1); }

    /**
     * Throws UnsupportedOperationException. SequenceLocks
     * do not support Condition objects.
     *
     * @throws UnsupportedOperationException always
     */
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    /**
     * Queries the number of holds on this lock by the current thread.
     *
     * <p>A thread has a hold on a lock for each lock action that is not
     * matched by an unlock action.
     *
     * <p>The hold count information is typically only used for testing and
     * debugging purposes.
     *
     * @return the number of holds on this lock by the current thread,
     *         or zero if this lock is not held by the current thread
     */
    public long getHoldCount() { return sync.getHoldCount(); }

    /**
     * Queries if this lock is held by the current thread.
     *
     * @return {@code true} if current thread holds this lock and
     *         {@code false} otherwise
     */
    public boolean isHeldByCurrentThread() { return sync.isHeldExclusively(); }

    /**
     * Queries if this lock is held by any thread. This method is
     * designed for use in monitoring of the system state,
     * not for synchronization control.
     *
     * @return {@code true} if any thread holds this lock and
     *         {@code false} otherwise
     */
    public boolean isLocked() { return sync.isLocked(); }

    /**
     * Returns the thread that currently owns this lock, or
     * {@code null} if not owned. When this method is called by a
     * thread that is not the owner, the return value reflects a
     * best-effort approximation of current lock status. For example,
     * the owner may be momentarily {@code null} even if there are
     * threads trying to acquire the lock but have not yet done so.
     * This method is designed to facilitate construction of
     * subclasses that provide more extensive lock monitoring
     * facilities.
     *
     * @return the owner, or {@code null} if not owned
     */
    protected Thread getOwner() { return sync.getOwner(); }

    /**
     * Queries whether any threads are waiting to acquire this lock. Note that
     * because cancellations may occur at any time, a {@code true}
     * return does not guarantee that any other thread will ever
     * acquire this lock.  This method is designed primarily for use in
     * monitoring of the system state.
     *
     * @return {@code true} if there may be other threads waiting to
     *         acquire the lock
     */
    public final boolean hasQueuedThreads() {
        return sync.hasQueuedThreads();
    }

    /**
     * Queries whether the given thread is waiting to acquire this
     * lock. Note that because cancellations may occur at any time, a
     * {@code true} return does not guarantee that this thread
     * will ever acquire this lock.  This method is designed primarily for use
     * in monitoring of the system state.
     *
     * @param thread the thread
     * @return {@code true} if the given thread is queued waiting for this lock
     * @throws NullPointerException if the thread is null
     */
    public final boolean hasQueuedThread(Thread thread) {
        return sync.isQueued(thread);
    }

    /**
     * Returns an estimate of the number of threads waiting to
     * acquire this lock.  The value is only an estimate because the number of
     * threads may change dynamically while this method traverses
     * internal data structures.  This method is designed for use in
     * monitoring of the system state, not for synchronization
     * control.
     *
     * @return the estimated number of threads waiting for this lock
     */
    public final int getQueueLength() {
        return sync.getQueueLength();
    }

    /**
     * Returns a collection containing threads that may be waiting to
     * acquire this lock.  Because the actual set of threads may change
     * dynamically while constructing this result, the returned
     * collection is only a best-effort estimate.  The elements of the
     * returned collection are in no particular order.  This method is
     * designed to facilitate construction of subclasses that provide
     * more extensive monitoring facilities.
     *
     * @return the collection of threads
     */
    protected Collection<Thread> getQueuedThreads() {
        return sync.getQueuedThreads();
    }

    /**
     * Returns a string identifying this lock, as well as its lock state.
     * The state, in brackets, includes either the String {@code "Unlocked"}
     * or the String {@code "Locked by"} followed by the
     * {@linkplain Thread#getName name} of the owning thread.
     *
     * @return a string identifying this lock, as well as its lock state
     */
    public String toString() {
        Thread o = sync.getOwner();
        return super.toString() + ((o == null) ?
                                   "[Unlocked]" :
                                   "[Locked by thread " + o.getName() + "]");
    }

}
