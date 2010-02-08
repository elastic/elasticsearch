/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Written by Cliff Click and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

package org.elasticsearch.util.concurrent.highscalelib;

import sun.misc.Unsafe;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * An auto-resizing table of {@code longs}, supporting low-contention CAS
 * operations.  Updates are done with CAS's to no particular table element.
 * The intent is to support highly scalable counters, r/w locks, and other
 * structures where the updates are associative, loss-free (no-brainer), and
 * otherwise happen at such a high volume that the cache contention for
 * CAS'ing a single word is unacceptable.
 * <p/>
 * <p>This API is overkill for simple counters (e.g. no need for the 'mask')
 * and is untested as an API for making a scalable r/w lock and so is likely
 * to change!
 *
 * @author Cliff Click
 * @since 1.5
 */


public class ConcurrentAutoTable implements Serializable {

    // --- public interface ---

    /**
     * Add the given value to current counter value.  Concurrent updates will
     * not be lost, but addAndGet or getAndAdd are not implemented because the
     * total counter value (i.e., {@link #get}) is not atomically updated.
     * Updates are striped across an array of counters to avoid cache contention
     * and has been tested with performance scaling linearly up to 768 CPUs.
     */
    public void add(long x) {
        add_if_mask(x, 0);
    }

    /**
     * {@link #add} with -1
     */
    public void decrement() {
        add_if_mask(-1L, 0);
    }

    /**
     * {@link #add} with +1
     */
    public void increment() {
        add_if_mask(1L, 0);
    }

    /**
     * Atomically set the sum of the striped counters to specified value.
     * Rather more expensive than a simple store, in order to remain atomic.
     */
    public void set(long x) {
        CAT newcat = new CAT(null, 4, x);
        // Spin until CAS works
        while (!CAS_cat(_cat, newcat)) ;
    }

    /**
     * Current value of the counter.  Since other threads are updating furiously
     * the value is only approximate, but it includes all counts made by the
     * current thread.  Requires a pass over the internally striped counters.
     */
    public long get() {
        return _cat.sum(0);
    }

    /**
     * Same as {@link #get}, included for completeness.
     */
    public int intValue() {
        return (int) _cat.sum(0);
    }

    /**
     * Same as {@link #get}, included for completeness.
     */
    public long longValue() {
        return _cat.sum(0);
    }

    /**
     * A cheaper {@link #get}.  Updated only once/millisecond, but as fast as a
     * simple load instruction when not updating.
     */
    public long estimate_get() {
        return _cat.estimate_sum(0);
    }

    /**
     * Return the counter's {@code long} value converted to a string.
     */
    public String toString() {
        return _cat.toString(0);
    }

    /**
     * A more verbose print than {@link #toString}, showing internal structure.
     * Useful for debugging.
     */
    public void print() {
        _cat.print();
    }

    /**
     * Return the internal counter striping factor.  Useful for diagnosing
     * performance problems.
     */
    public int internal_size() {
        return _cat._t.length;
    }

    // Only add 'x' to some slot in table, hinted at by 'hash', if bits under
    // the mask are all zero.  The sum can overflow or 'x' can contain bits in
    // the mask. Value is CAS'd so no counts are lost.  The CAS is retried until
    // it succeeds or bits are found under the mask.  Returned value is the old
    // value - which WILL have zero under the mask on success and WILL NOT have
    // zero under the mask for failure.

    private long add_if_mask(long x, long mask) {
        return _cat.add_if_mask(x, mask, hash(), this);
    }

    // The underlying array of concurrently updated long counters
    private volatile CAT _cat = new CAT(null, 4/*Start Small, Think Big!*/, 0L);
    private static final AtomicReferenceFieldUpdater<ConcurrentAutoTable, CAT> _catUpdater =
            AtomicReferenceFieldUpdater.newUpdater(ConcurrentAutoTable.class, CAT.class, "_cat");

    private boolean CAS_cat(CAT oldcat, CAT newcat) {
        return _catUpdater.compareAndSet(this, oldcat, newcat);
    }

    // Hash spreader

    private static final int hash() {
        int h = System.identityHashCode(Thread.currentThread());
        // You would think that System.identityHashCode on the current thread
        // would be a good hash fcn, but actually on SunOS 5.8 it is pretty lousy
        // in the low bits.
        h ^= (h >>> 20) ^ (h >>> 12);   // Bit spreader, borrowed from Doug Lea
        h ^= (h >>> 7) ^ (h >>> 4);
        return h << 2;                // Pad out cache lines.  The goal is to avoid cache-line contention
    }

    // --- CAT -----------------------------------------------------------------

    private static class CAT implements Serializable {

        // Unsafe crud: get a function which will CAS arrays
        private static final Unsafe _unsafe = UtilUnsafe.getUnsafe();
        private static final int _Lbase = _unsafe.arrayBaseOffset(long[].class);
        private static final int _Lscale = _unsafe.arrayIndexScale(long[].class);

        private static long rawIndex(long[] ary, int i) {
            assert i >= 0 && i < ary.length;
            return _Lbase + i * _Lscale;
        }

        private final static boolean CAS(long[] A, int idx, long old, long nnn) {
            return _unsafe.compareAndSwapLong(A, rawIndex(A, idx), old, nnn);
        }

        volatile long _resizers;    // count of threads attempting a resize
        static private final AtomicLongFieldUpdater<CAT> _resizerUpdater =
                AtomicLongFieldUpdater.newUpdater(CAT.class, "_resizers");

        private final CAT _next;
        private volatile long _sum_cache;
        private volatile long _fuzzy_sum_cache;
        private volatile long _fuzzy_time;
        private static final int MAX_SPIN = 2;
        private long[] _t;            // Power-of-2 array of longs

        CAT(CAT next, int sz, long init) {
            _next = next;
            _sum_cache = Long.MIN_VALUE;
            _t = new long[sz];
            _t[0] = init;
        }

        // Only add 'x' to some slot in table, hinted at by 'hash', if bits under
        // the mask are all zero.  The sum can overflow or 'x' can contain bits in
        // the mask.  Value is CAS'd so no counts are lost.  The CAS is attempted
        // ONCE.

        public long add_if_mask(long x, long mask, int hash, ConcurrentAutoTable master) {
            long[] t = _t;
            int idx = hash & (t.length - 1);
            // Peel loop; try once fast
            long old = t[idx];
            boolean ok = CAS(t, idx, old & ~mask, old + x);
            if (_sum_cache != Long.MIN_VALUE)
                _sum_cache = Long.MIN_VALUE; // Blow out cache
            if (ok) return old;      // Got it
            if ((old & mask) != 0) return old; // Failed for bit-set under mask
            // Try harder
            int cnt = 0;
            while (true) {
                old = t[idx];
                if ((old & mask) != 0) return old; // Failed for bit-set under mask
                if (CAS(t, idx, old, old + x)) break; // Got it!
                cnt++;
            }
            if (cnt < MAX_SPIN) return old; // Allowable spin loop count
            if (t.length >= 1024 * 1024) return old; // too big already

            // Too much contention; double array size in an effort to reduce contention
            long r = _resizers;
            int newbytes = (t.length << 1) << 3/*word to bytes*/;
            while (!_resizerUpdater.compareAndSet(this, r, r + newbytes))
                r = _resizers;
            r += newbytes;
            if (master._cat != this) return old; // Already doubled, don't bother
            if ((r >> 17) != 0) {      // Already too much allocation attempts?
                // TODO - use a wait with timeout, so we'll wakeup as soon as the new
                // table is ready, or after the timeout in any case.  Annoyingly, this
                // breaks the non-blocking property - so for now we just briefly sleep.
                //synchronized( this ) { wait(8*megs); }         // Timeout - we always wakeup
                try {
                    Thread.sleep(r >> 17);
                } catch (InterruptedException e) {
                }
                if (master._cat != this) return old;
            }

            CAT newcat = new CAT(this, t.length * 2, 0);
            // Take 1 stab at updating the CAT with the new larger size.  If this
            // fails, we assume some other thread already expanded the CAT - so we
            // do not need to retry until it succeeds.
            master.CAS_cat(this, newcat);
            return old;
        }


        // Return the current sum of all things in the table, stripping off mask
        // before the add.  Writers can be updating the table furiously, so the
        // sum is only locally accurate.

        public long sum(long mask) {
            long sum = _sum_cache;
            if (sum != Long.MIN_VALUE) return sum;
            sum = _next == null ? 0 : _next.sum(mask); // Recursively get cached sum
            long[] t = _t;
            for (int i = 0; i < t.length; i++)
                sum += t[i] & (~mask);
            _sum_cache = sum;         // Cache includes recursive counts
            return sum;
        }

        // Fast fuzzy version.  Used a cached value until it gets old, then re-up
        // the cache.

        public long estimate_sum(long mask) {
            // For short tables, just do the work
            if (_t.length <= 64) return sum(mask);
            // For bigger tables, periodically freshen a cached value
            long millis = System.currentTimeMillis();
            if (_fuzzy_time != millis) { // Time marches on?
                _fuzzy_sum_cache = sum(mask); // Get sum the hard way
                _fuzzy_time = millis;   // Indicate freshness of cached value
            }
            return _fuzzy_sum_cache;  // Return cached sum
        }

        // Update all table slots with CAS.

        public void all_or(long mask) {
            long[] t = _t;
            for (int i = 0; i < t.length; i++) {
                boolean done = false;
                while (!done) {
                    long old = t[i];
                    done = CAS(t, i, old, old | mask);
                }
            }
            if (_next != null) _next.all_or(mask);
            if (_sum_cache != Long.MIN_VALUE)
                _sum_cache = Long.MIN_VALUE; // Blow out cache
        }

        public void all_and(long mask) {
            long[] t = _t;
            for (int i = 0; i < t.length; i++) {
                boolean done = false;
                while (!done) {
                    long old = t[i];
                    done = CAS(t, i, old, old & mask);
                }
            }
            if (_next != null) _next.all_and(mask);
            if (_sum_cache != Long.MIN_VALUE)
                _sum_cache = Long.MIN_VALUE; // Blow out cache
        }

        // Set/stomp all table slots.  No CAS.

        public void all_set(long val) {
            long[] t = _t;
            for (int i = 0; i < t.length; i++)
                t[i] = val;
            if (_next != null) _next.all_set(val);
            if (_sum_cache != Long.MIN_VALUE)
                _sum_cache = Long.MIN_VALUE; // Blow out cache
        }

        String toString(long mask) {
            return Long.toString(sum(mask));
        }

        public void print() {
            long[] t = _t;
            System.out.print("[sum=" + _sum_cache + "," + t[0]);
            for (int i = 1; i < t.length; i++)
                System.out.print("," + t[i]);
            System.out.print("]");
            if (_next != null) _next.print();
        }
    }
}

