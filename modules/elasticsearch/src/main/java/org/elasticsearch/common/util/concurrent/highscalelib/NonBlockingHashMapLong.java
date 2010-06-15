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

package org.elasticsearch.common.util.concurrent.highscalelib;

import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import sun.misc.Unsafe;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * A lock-free alternate implementation of {@link java.util.ConcurrentHashMap}
 * with <strong>primitive long keys</strong>, better scaling properties and
 * generally lower costs.  The use of {@code long} keys allows for faster
 * compares and lower memory costs.  The Map provides identical correctness
 * properties as ConcurrentHashMap.  All operations are non-blocking and
 * multi-thread safe, including all update operations.  {@link
 * NonBlockingHashMapLong} scales substatially better than {@link
 * java.util.ConcurrentHashMap} for high update rates, even with a large
 * concurrency factor.  Scaling is linear up to 768 CPUs on a 768-CPU Azul
 * box, even with 100% updates or 100% reads or any fraction in-between.
 * Linear scaling up to all cpus has been observed on a 32-way Sun US2 box,
 * 32-way Sun Niagra box, 8-way Intel box and a 4-way Power box.
 * <p/>
 * <p><strong>The main benefit of this class</strong> over using plain {@link
 * org.cliffc.high_scale_lib.NonBlockingHashMap} with {@link Long} keys is
 * that it avoids the auto-boxing and unboxing costs.  Since auto-boxing is
 * <em>automatic</em>, it is easy to accidentally cause auto-boxing and negate
 * the space and speed benefits.
 * <p/>
 * <p>This class obeys the same functional specification as {@link
 * java.util.Hashtable}, and includes versions of methods corresponding to
 * each method of <tt>Hashtable</tt>.  However, even though all operations are
 * thread-safe, operations do <em>not</em> entail locking and there is
 * <em>not</em> any support for locking the entire table in a way that
 * prevents all access.  This class is fully interoperable with
 * <tt>Hashtable</tt> in programs that rely on its thread safety but not on
 * its synchronization details.
 * <p/>
 * <p> Operations (including <tt>put</tt>) generally do not block, so may
 * overlap with other update operations (including other <tt>puts</tt> and
 * <tt>removes</tt>).  Retrievals reflect the results of the most recently
 * <em>completed</em> update operations holding upon their onset.  For
 * aggregate operations such as <tt>putAll</tt>, concurrent retrievals may
 * reflect insertion or removal of only some entries.  Similarly, Iterators
 * and Enumerations return elements reflecting the state of the hash table at
 * some point at or since the creation of the iterator/enumeration.  They do
 * <em>not</em> throw {@link ConcurrentModificationException}.  However,
 * iterators are designed to be used by only one thread at a time.
 * <p/>
 * <p> Very full tables, or tables with high reprobe rates may trigger an
 * internal resize operation to move into a larger table.  Resizing is not
 * terribly expensive, but it is not free either; during resize operations
 * table throughput may drop somewhat.  All threads that visit the table
 * during a resize will 'help' the resizing but will still be allowed to
 * complete their operation before the resize is finished (i.e., a simple
 * 'get' operation on a million-entry table undergoing resizing will not need
 * to block until the entire million entries are copied).
 * <p/>
 * <p>This class and its views and iterators implement all of the
 * <em>optional</em> methods of the {@link Map} and {@link Iterator}
 * interfaces.
 * <p/>
 * <p> Like {@link Hashtable} but unlike {@link HashMap}, this class
 * does <em>not</em> allow <tt>null</tt> to be used as a value.
 *
 * @author Cliff Click
 * @param <TypeV> the type of mapped values
 * @since 1.5
 */

public class NonBlockingHashMapLong<TypeV>
        extends AbstractMap<Long, TypeV>
        implements ConcurrentMap<Long, TypeV>, Serializable, ConcurrentMapLong<TypeV> {

    private static final long serialVersionUID = 1234123412341234124L;

    private static final int REPROBE_LIMIT = 10; // Too many reprobes then force a table-resize

    // --- Bits to allow Unsafe access to arrays
    private static final Unsafe _unsafe = UtilUnsafe.getUnsafe();
    private static final int _Obase = _unsafe.arrayBaseOffset(Object[].class);
    private static final int _Oscale = _unsafe.arrayIndexScale(Object[].class);

    private static long rawIndex(final Object[] ary, final int idx) {
        assert idx >= 0 && idx < ary.length;
        return _Obase + idx * _Oscale;
    }

    private static final int _Lbase = _unsafe.arrayBaseOffset(long[].class);
    private static final int _Lscale = _unsafe.arrayIndexScale(long[].class);

    private static long rawIndex(final long[] ary, final int idx) {
        assert idx >= 0 && idx < ary.length;
        return _Lbase + idx * _Lscale;
    }

    // --- Bits to allow Unsafe CAS'ing of the CHM field
    private static final long _chm_offset;
    private static final long _val_1_offset;

    static {                      // <clinit>
        Field f = null;
        try {
            f = NonBlockingHashMapLong.class.getDeclaredField("_chm");
        }
        catch (java.lang.NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        _chm_offset = _unsafe.objectFieldOffset(f);

        try {
            f = NonBlockingHashMapLong.class.getDeclaredField("_val_1");
        }
        catch (java.lang.NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        _val_1_offset = _unsafe.objectFieldOffset(f);
    }

    private final boolean CAS(final long offset, final Object old, final Object nnn) {
        return _unsafe.compareAndSwapObject(this, offset, old, nnn);
    }

    // --- Adding a 'prime' bit onto Values via wrapping with a junk wrapper class

    private static final class Prime {
        final Object _V;

        Prime(Object V) {
            _V = V;
        }

        static Object unbox(Object V) {
            return V instanceof Prime ? ((Prime) V)._V : V;
        }
    }

    // --- The Hash Table --------------------
    private transient CHM _chm;
    // This next field holds the value for Key 0 - the special key value which
    // is the initial array value, and also means: no-key-inserted-yet.
    private transient Object _val_1; // Value for Key: NO_KEY

    // Time since last resize
    private transient long _last_resize_milli;

    // Optimize for space: use a 1/2-sized table and allow more re-probes
    private final boolean _opt_for_space;

    // --- Minimum table size ----------------
    // Pick size 16 K/V pairs, which turns into (16*2)*4+12 = 140 bytes on a
    // standard 32-bit HotSpot, and (16*2)*8+12 = 268 bytes on 64-bit Azul.
    private static final int MIN_SIZE_LOG = 4;             //
    private static final int MIN_SIZE = (1 << MIN_SIZE_LOG); // Must be power of 2

    // --- Sentinels -------------------------
    // No-Match-Old - putIfMatch does updates only if it matches the old value,
    // and NO_MATCH_OLD basically counts as a wildcard match.
    private static final Object NO_MATCH_OLD = new Object(); // Sentinel
    // Match-Any-not-null - putIfMatch does updates only if it find a real old
    // value.
    private static final Object MATCH_ANY = new Object(); // Sentinel
    // This K/V pair has been deleted (but the Key slot is forever claimed).
    // The same Key can be reinserted with a new value later.
    private static final Object TOMBSTONE = new Object();
    // Prime'd or box'd version of TOMBSTONE.  This K/V pair was deleted, then a
    // table resize started.  The K/V pair has been marked so that no new
    // updates can happen to the old table (and since the K/V pair was deleted
    // nothing was copied to the new table).
    private static final Prime TOMBPRIME = new Prime(TOMBSTONE);

    // I exclude 1 long from the 2^64 possibilities, and test for it before
    // entering the main array.  The NO_KEY value must be zero, the initial
    // value set by Java before it hands me the array.
    private static final long NO_KEY = 0L;

    // --- dump ----------------------------------------------------------------

    /**
     * Verbose printout of table internals, useful for debugging.
     */
    public final void print() {
        System.out.println("=========");
        print_impl(-99, NO_KEY, _val_1);
        _chm.print();
        System.out.println("=========");
    }

    private static final void print_impl(final int i, final long K, final Object V) {
        String p = (V instanceof Prime) ? "prime_" : "";
        Object V2 = Prime.unbox(V);
        String VS = (V2 == TOMBSTONE) ? "tombstone" : V2.toString();
        System.out.println("[" + i + "]=(" + K + "," + p + VS + ")");
    }

    private final void print2() {
        System.out.println("=========");
        print2_impl(-99, NO_KEY, _val_1);
        _chm.print();
        System.out.println("=========");
    }

    private static final void print2_impl(final int i, final long K, final Object V) {
        if (V != null && Prime.unbox(V) != TOMBSTONE)
            print_impl(i, K, V);
    }

    // Count of reprobes
    private transient Counter _reprobes = new Counter();

    /**
     * Get and clear the current count of reprobes.  Reprobes happen on key
     * collisions, and a high reprobe rate may indicate a poor hash function or
     * weaknesses in the table resizing function.
     *
     * @return the count of reprobes since the last call to {@link #reprobes}
     *         or since the table was created.
     */
    public long reprobes() {
        long r = _reprobes.get();
        _reprobes = new Counter();
        return r;
    }


    // --- reprobe_limit -----------------------------------------------------
    // Heuristic to decide if we have reprobed toooo many times.  Running over
    // the reprobe limit on a 'get' call acts as a 'miss'; on a 'put' call it
    // can trigger a table resize.  Several places must have exact agreement on
    // what the reprobe_limit is, so we share it here.

    private static final int reprobe_limit(int len) {
        return REPROBE_LIMIT + (len >> 2);
    }

    // --- NonBlockingHashMapLong ----------------------------------------------
    // Constructors

    /**
     * Create a new NonBlockingHashMapLong with default minimum size (currently set
     * to 8 K/V pairs or roughly 84 bytes on a standard 32-bit JVM).
     */
    public NonBlockingHashMapLong() {
        this(MIN_SIZE, true);
    }

    /**
     * Create a new NonBlockingHashMapLong with initial room for the given
     * number of elements, thus avoiding internal resizing operations to reach
     * an appropriate size.  Large numbers here when used with a small count of
     * elements will sacrifice space for a small amount of time gained.  The
     * initial size will be rounded up internally to the next larger power of 2.
     */
    public NonBlockingHashMapLong(final int initial_sz) {
        this(initial_sz, true);
    }

    /**
     * Create a new NonBlockingHashMapLong, setting the space-for-speed
     * tradeoff.  {@code true} optimizes for space and is the default.  {@code
     * false} optimizes for speed and doubles space costs for roughly a 10%
     * speed improvement.
     */
    public NonBlockingHashMapLong(final boolean opt_for_space) {
        this(1, opt_for_space);
    }

    /**
     * Create a new NonBlockingHashMapLong, setting both the initial size and
     * the space-for-speed tradeoff.  {@code true} optimizes for space and is
     * the default.  {@code false} optimizes for speed and doubles space costs
     * for roughly a 10% speed improvement.
     */
    public NonBlockingHashMapLong(final int initial_sz, final boolean opt_for_space) {
        _opt_for_space = opt_for_space;
        initialize(initial_sz);
    }

    private final void initialize(final int initial_sz) {
        if (initial_sz < 0) throw new IllegalArgumentException();
        int i;                      // Convert to next largest power-of-2
        for (i = MIN_SIZE_LOG; (1 << i) < initial_sz; i++) ;
        _chm = new CHM(this, new Counter(), i);
        _val_1 = TOMBSTONE;         // Always as-if deleted
        _last_resize_milli = System.currentTimeMillis();
    }

    // --- wrappers ------------------------------------------------------------

    /**
     * Returns the number of key-value mappings in this map.
     *
     * @return the number of key-value mappings in this map
     */
    public int size() {
        return (_val_1 == TOMBSTONE ? 0 : 1) + (int) _chm.size();
    }

    /**
     * Tests if the key in the table.
     *
     * @return <tt>true</tt> if the key is in the table
     */
    public boolean containsKey(long key) {
        return get(key) != null;
    }

    /**
     * Legacy method testing if some key maps into the specified value in this
     * table.  This method is identical in functionality to {@link
     * #containsValue}, and exists solely to ensure full compatibility with
     * class {@link java.util.Hashtable}, which supported this method prior to
     * introduction of the Java Collections framework.
     *
     * @param val a value to search for
     * @return <tt>true</tt> if this map maps one or more keys to the specified value
     * @throws NullPointerException if the specified value is null
     */
    public boolean contains(Object val) {
        return containsValue(val);
    }

    /**
     * Maps the specified key to the specified value in the table.  The value
     * cannot be null.  <p> The value can be retrieved by calling {@link #get}
     * with a key that is equal to the original key.
     *
     * @param key key with which the specified value is to be associated
     * @param val value to be associated with the specified key
     * @return the previous value associated with <tt>key</tt>, or
     *         <tt>null</tt> if there was no mapping for <tt>key</tt>
     * @throws NullPointerException if the specified value is null
     */
    public TypeV put(long key, TypeV val) {
        return putIfMatch(key, val, NO_MATCH_OLD);
    }

    /**
     * Atomically, do a {@link #put} if-and-only-if the key is not mapped.
     * Useful to ensure that only a single mapping for the key exists, even if
     * many threads are trying to create the mapping in parallel.
     *
     * @return the previous value associated with the specified key,
     *         or <tt>null</tt> if there was no mapping for the key
     * @throws NullPointerException if the specified is value is null
     */
    public TypeV putIfAbsent(long key, TypeV val) {
        return putIfMatch(key, val, TOMBSTONE);
    }

    /**
     * Removes the key (and its corresponding value) from this map.
     * This method does nothing if the key is not in the map.
     *
     * @return the previous value associated with <tt>key</tt>, or
     *         <tt>null</tt> if there was no mapping for <tt>key</tt>
     */
    public TypeV remove(long key) {
        return putIfMatch(key, TOMBSTONE, NO_MATCH_OLD);
    }

    /**
     * Atomically do a {@link #remove(long)} if-and-only-if the key is mapped
     * to a value which is <code>equals</code> to the given value.
     *
     * @throws NullPointerException if the specified value is null
     */
    public boolean remove(long key, Object val) {
        return putIfMatch(key, TOMBSTONE, val) == val;
    }

    /**
     * Atomically do a <code>put(key,val)</code> if-and-only-if the key is
     * mapped to some value already.
     *
     * @throws NullPointerException if the specified value is null
     */
    public TypeV replace(long key, TypeV val) {
        return putIfMatch(key, val, MATCH_ANY);
    }

    /**
     * Atomically do a <code>put(key,newValue)</code> if-and-only-if the key is
     * mapped a value which is <code>equals</code> to <code>oldValue</code>.
     *
     * @throws NullPointerException if the specified value is null
     */
    public boolean replace(long key, TypeV oldValue, TypeV newValue) {
        return putIfMatch(key, newValue, oldValue) == oldValue;
    }

    private final TypeV putIfMatch(long key, Object newVal, Object oldVal) {
        if (oldVal == null || newVal == null) throw new NullPointerException();
        if (key == NO_KEY) {
            final Object curVal = _val_1;
            if (oldVal == NO_MATCH_OLD || // Do we care about expected-Value at all?
                    curVal == oldVal ||       // No instant match already?
                    (oldVal == MATCH_ANY && curVal != TOMBSTONE) ||
                    oldVal.equals(curVal))   // Expensive equals check
                CAS(_val_1_offset, curVal, newVal); // One shot CAS update attempt
            return curVal == TOMBSTONE ? null : (TypeV) curVal; // Return the last value present
        }
        final Object res = _chm.putIfMatch(key, newVal, oldVal);
        assert !(res instanceof Prime);
        assert res != null;
        return res == TOMBSTONE ? null : (TypeV) res;
    }

    /**
     * Removes all of the mappings from this map.
     */
    public void clear() {         // Smack a new empty table down
        CHM newchm = new CHM(this, new Counter(), MIN_SIZE_LOG);
        while (!CAS(_chm_offset, _chm, newchm)) // Spin until the clear works
            ;
        CAS(_val_1_offset, _val_1, TOMBSTONE);
    }

    /**
     * Returns <tt>true</tt> if this Map maps one or more keys to the specified
     * value.  <em>Note</em>: This method requires a full internal traversal of the
     * hash table and is much slower than {@link #containsKey}.
     *
     * @param val value whose presence in this map is to be tested
     * @return <tt>true</tt> if this Map maps one or more keys to the specified value
     * @throws NullPointerException if the specified value is null
     */
    public boolean containsValue(Object val) {
        if (val == null) return false;
        if (val == _val_1) return true; // Key 0
        for (TypeV V : values())
            if (V == val || V.equals(val))
                return true;
        return false;
    }

    // --- get -----------------------------------------------------------------

    /**
     * Returns the value to which the specified key is mapped, or {@code null}
     * if this map contains no mapping for the key.
     * <p>More formally, if this map contains a mapping from a key {@code k} to
     * a value {@code v} such that {@code key==k}, then this method
     * returns {@code v}; otherwise it returns {@code null}.  (There can be at
     * most one such mapping.)
     *
     * @throws NullPointerException if the specified key is null
     */
    // Never returns a Prime nor a Tombstone.
    public final TypeV get(long key) {
        if (key == NO_KEY) {
            final Object V = _val_1;
            return V == TOMBSTONE ? null : (TypeV) V;
        }
        final Object V = _chm.get_impl(key);
        assert !(V instanceof Prime); // Never return a Prime
        assert V != TOMBSTONE;
        return (TypeV) V;
    }

    /**
     * Auto-boxing version of {@link #get(long)}.
     */
    public TypeV get(Object key) {
        return (key instanceof Long) ? get(((Long) key).longValue()) : null;
    }

    /**
     * Auto-boxing version of {@link #remove(long)}.
     */
    public TypeV remove(Object key) {
        return (key instanceof Long) ? remove(((Long) key).longValue()) : null;
    }

    /**
     * Auto-boxing version of {@link #remove(long,Object)}.
     */
    public boolean remove(Object key, Object Val) {
        return (key instanceof Long) ? remove(((Long) key).longValue(), Val) : false;
    }

    /**
     * Auto-boxing version of {@link #containsKey(long)}.
     */
    public boolean containsKey(Object key) {
        return (key instanceof Long) ? containsKey(((Long) key).longValue()) : false;
    }

    /**
     * Auto-boxing version of {@link #putIfAbsent}.
     */
    public TypeV putIfAbsent(Long key, TypeV val) {
        return putIfAbsent(((Long) key).longValue(), val);
    }

    /**
     * Auto-boxing version of {@link #replace}.
     */
    public TypeV replace(Long key, TypeV Val) {
        return replace(((Long) key).longValue(), Val);
    }

    /**
     * Auto-boxing version of {@link #put}.
     */
    public TypeV put(Long key, TypeV val) {
        return put(key.longValue(), val);
    }

    /**
     * Auto-boxing version of {@link #replace}.
     */
    public boolean replace(Long key, TypeV oldValue, TypeV newValue) {
        return replace(((Long) key).longValue(), oldValue, newValue);
    }

    // --- help_copy -----------------------------------------------------------
    // Help along an existing resize operation.  This is just a fast cut-out
    // wrapper, to encourage inlining for the fast no-copy-in-progress case.  We
    // always help the top-most table copy, even if there are nested table
    // copies in progress.

    private final void help_copy() {
        // Read the top-level CHM only once.  We'll try to help this copy along,
        // even if it gets promoted out from under us (i.e., the copy completes
        // and another KVS becomes the top-level copy).
        CHM topchm = _chm;
        if (topchm._newchm == null) return; // No copy in-progress
        topchm.help_copy_impl(false);
    }


    // --- CHM -----------------------------------------------------------------
    // The control structure for the NonBlockingHashMapLong

    private static final class CHM<TypeV> implements Serializable {
        // Back-pointer to top-level structure
        final NonBlockingHashMapLong _nbhml;

        // Size in active K,V pairs
        private final Counter _size;

        public int size() {
            return (int) _size.get();
        }

        // ---
        // These next 2 fields are used in the resizing heuristics, to judge when
        // it is time to resize or copy the table.  Slots is a count of used-up
        // key slots, and when it nears a large fraction of the table we probably
        // end up reprobing too much.  Last-resize-milli is the time since the
        // last resize; if we are running back-to-back resizes without growing
        // (because there are only a few live keys but many slots full of dead
        // keys) then we need a larger table to cut down on the churn.

        // Count of used slots, to tell when table is full of dead unusable slots
        private final Counter _slots;

        public int slots() {
            return (int) _slots.get();
        }

        // ---
        // New mappings, used during resizing.
        // The 'next' CHM - created during a resize operation.  This represents
        // the new table being copied from the old one.  It's the volatile
        // variable that is read as we cross from one table to the next, to get
        // the required memory orderings.  It monotonically transits from null to
        // set (once).
        volatile CHM _newchm;
        private static final AtomicReferenceFieldUpdater<CHM, CHM> _newchmUpdater =
                AtomicReferenceFieldUpdater.newUpdater(CHM.class, CHM.class, "_newchm");

        // Set the _newchm field if we can.  AtomicUpdaters do not fail spuriously.

        boolean CAS_newchm(CHM newchm) {
            return _newchmUpdater.compareAndSet(this, null, newchm);
        }

        // Sometimes many threads race to create a new very large table.  Only 1
        // wins the race, but the losers all allocate a junk large table with
        // hefty allocation costs.  Attempt to control the overkill here by
        // throttling attempts to create a new table.  I cannot really block here
        // (lest I lose the non-blocking property) but late-arriving threads can
        // give the initial resizing thread a little time to allocate the initial
        // new table.  The Right Long Term Fix here is to use array-lets and
        // incrementally create the new very large array.  In C I'd make the array
        // with malloc (which would mmap under the hood) which would only eat
        // virtual-address and not real memory - and after Somebody wins then we
        // could in parallel initialize the array.  Java does not allow
        // un-initialized array creation (especially of ref arrays!).
        volatile long _resizers;    // count of threads attempting an initial resize
        private static final AtomicLongFieldUpdater<CHM> _resizerUpdater =
                AtomicLongFieldUpdater.newUpdater(CHM.class, "_resizers");

        // --- key,val -------------------------------------------------------------
        // Access K,V for a given idx

        private final boolean CAS_key(int idx, long old, long key) {
            return _unsafe.compareAndSwapLong(_keys, rawIndex(_keys, idx), old, key);
        }

        private final boolean CAS_val(int idx, Object old, Object val) {
            return _unsafe.compareAndSwapObject(_vals, rawIndex(_vals, idx), old, val);
        }

        final long[] _keys;
        final Object[] _vals;

        // Simple constructor

        CHM(final NonBlockingHashMapLong nbhml, Counter size, final int logsize) {
            _nbhml = nbhml;
            _size = size;
            _slots = new Counter();
            _keys = new long[1 << logsize];
            _vals = new Object[1 << logsize];
        }

        // --- print innards

        private final void print() {
            for (int i = 0; i < _keys.length; i++) {
                long K = _keys[i];
                if (K != NO_KEY)
                    print_impl(i, K, _vals[i]);
            }
            CHM newchm = _newchm;     // New table, if any
            if (newchm != null) {
                System.out.println("----");
                newchm.print();
            }
        }

        // --- print only the live objects

        private final void print2() {
            for (int i = 0; i < _keys.length; i++) {
                long K = _keys[i];
                if (K != NO_KEY)       // key is sane
                    print2_impl(i, K, _vals[i]);
            }
            CHM newchm = _newchm;     // New table, if any
            if (newchm != null) {
                System.out.println("----");
                newchm.print2();
            }
        }

        // --- get_impl ----------------------------------------------------------
        // Never returns a Prime nor a Tombstone.

        private final Object get_impl(final long key) {
            final int len = _keys.length;
            int idx = (int) (key & (len - 1)); // First key hash

            // Main spin/reprobe loop, looking for a Key hit
            int reprobe_cnt = 0;
            while (true) {
                final long K = _keys[idx]; // Get key   before volatile read, could be NO_KEY
                final Object V = _vals[idx]; // Get value before volatile read, could be null or Tombstone or Prime
                if (K == NO_KEY) return null; // A clear miss

                // Key-compare
                if (key == K) {
                    // Key hit!  Check for no table-copy-in-progress
                    if (!(V instanceof Prime)) { // No copy?
                        if (V == TOMBSTONE) return null;
                        // We need a volatile-read between reading a newly inserted Value
                        // and returning the Value (so the user might end up reading the
                        // stale Value contents).
                        final CHM newchm = _newchm; // VOLATILE READ before returning V
                        return V;
                    }
                    // Key hit - but slot is (possibly partially) copied to the new table.
                    // Finish the copy & retry in the new table.
                    return copy_slot_and_check(idx, key).get_impl(key); // Retry in the new table
                }
                // get and put must have the same key lookup logic!  But only 'put'
                // needs to force a table-resize for a too-long key-reprobe sequence.
                // Check for too-many-reprobes on get.
                if (++reprobe_cnt >= reprobe_limit(len)) // too many probes
                    return _newchm == null // Table copy in progress?
                            ? null               // Nope!  A clear miss
                            : copy_slot_and_check(idx, key).get_impl(key); // Retry in the new table

                idx = (idx + 1) & (len - 1);    // Reprobe by 1!  (could now prefetch)
            }
        }

        // --- putIfMatch ---------------------------------------------------------
        // Put, Remove, PutIfAbsent, etc.  Return the old value.  If the returned
        // value is equal to expVal (or expVal is NO_MATCH_OLD) then the put can
        // be assumed to work (although might have been immediately overwritten).
        // Only the path through copy_slot passes in an expected value of null,
        // and putIfMatch only returns a null if passed in an expected null.

        private final Object putIfMatch(final long key, final Object putval, final Object expVal) {
            assert putval != null;
            assert !(putval instanceof Prime);
            assert !(expVal instanceof Prime);
            final int len = _keys.length;
            int idx = (int) (key & (len - 1)); // The first key

            // ---
            // Key-Claim stanza: spin till we can claim a Key (or force a resizing).
            int reprobe_cnt = 0;
            long K = NO_KEY;
            Object V = null;
            while (true) {           // Spin till we get a Key slot
                V = _vals[idx];         // Get old value
                K = _keys[idx];         // Get current key
                if (K == NO_KEY) {     // Slot is free?
                    // Found an empty Key slot - which means this Key has never been in
                    // this table.  No need to put a Tombstone - the Key is not here!
                    if (putval == TOMBSTONE) return putval; // Not-now & never-been in this table
                    // Claim the zero key-slot
                    if (CAS_key(idx, NO_KEY, key)) { // Claim slot for Key
                        _slots.add(1);      // Raise key-slots-used count
                        break;              // Got it!
                    }
                    // CAS to claim the key-slot failed.
                    //
                    // This re-read of the Key points out an annoying short-coming of Java
                    // CAS.  Most hardware CAS's report back the existing value - so that
                    // if you fail you have a *witness* - the value which caused the CAS
                    // to fail.  The Java API turns this into a boolean destroying the
                    // witness.  Re-reading does not recover the witness because another
                    // thread can write over the memory after the CAS.  Hence we can be in
                    // the unfortunate situation of having a CAS fail *for cause* but
                    // having that cause removed by a later store.  This turns a
                    // non-spurious-failure CAS (such as Azul has) into one that can
                    // apparently spuriously fail - and we avoid apparent spurious failure
                    // by not allowing Keys to ever change.
                    K = _keys[idx];       // CAS failed, get updated value
                    assert K != NO_KEY;  // If keys[idx] is NO_KEY, CAS shoulda worked
                }
                // Key slot was not null, there exists a Key here
                if (K == key)
                    break;                // Got it!

                // get and put must have the same key lookup logic!  Lest 'get' give
                // up looking too soon.
                //topmap._reprobes.add(1);
                if (++reprobe_cnt >= reprobe_limit(len)) {
                    // We simply must have a new table to do a 'put'.  At this point a
                    // 'get' will also go to the new table (if any).  We do not need
                    // to claim a key slot (indeed, we cannot find a free one to claim!).
                    final CHM newchm = resize();
                    if (expVal != null) _nbhml.help_copy(); // help along an existing copy
                    return newchm.putIfMatch(key, putval, expVal);
                }

                idx = (idx + 1) & (len - 1); // Reprobe!
            } // End of spinning till we get a Key slot

            // ---
            // Found the proper Key slot, now update the matching Value slot.  We
            // never put a null, so Value slots monotonically move from null to
            // not-null (deleted Values use Tombstone).  Thus if 'V' is null we
            // fail this fast cutout and fall into the check for table-full.
            if (putval == V) return V; // Fast cutout for no-change

            // See if we want to move to a new table (to avoid high average re-probe
            // counts).  We only check on the initial set of a Value from null to
            // not-null (i.e., once per key-insert).
            if ((V == null && tableFull(reprobe_cnt, len)) ||
                    // Or we found a Prime: resize is already in progress.  The resize
                    // call below will do a CAS on _newchm forcing the read.
                    V instanceof Prime) {
                resize();               // Force the new table copy to start
                return copy_slot_and_check(idx, expVal).putIfMatch(key, putval, expVal);
            }

            // ---
            // We are finally prepared to update the existing table
            while (true) {
                assert !(V instanceof Prime);

                // Must match old, and we do not?  Then bail out now.  Note that either V
                // or expVal might be TOMBSTONE.  Also V can be null, if we've never
                // inserted a value before.  expVal can be null if we are called from
                // copy_slot.

                if (expVal != NO_MATCH_OLD && // Do we care about expected-Value at all?
                        V != expVal &&        // No instant match already?
                        (expVal != MATCH_ANY || V == TOMBSTONE || V == null) &&
                        !(V == null && expVal == TOMBSTONE) &&    // Match on null/TOMBSTONE combo
                        (expVal == null || !expVal.equals(V))) // Expensive equals check at the last
                    return V;               // Do not update!

                // Actually change the Value in the Key,Value pair
                if (CAS_val(idx, V, putval)) {
                    // CAS succeeded - we did the update!
                    // Both normal put's and table-copy calls putIfMatch, but table-copy
                    // does not (effectively) increase the number of live k/v pairs.
                    if (expVal != null) {
                        // Adjust sizes - a striped counter
                        if ((V == null || V == TOMBSTONE) && putval != TOMBSTONE) _size.add(1);
                        if (!(V == null || V == TOMBSTONE) && putval == TOMBSTONE) _size.add(-1);
                    }
                    return (V == null && expVal != null) ? TOMBSTONE : V;
                }
                // Else CAS failed
                V = _vals[idx];         // Get new value
                // If a Prime'd value got installed, we need to re-run the put on the
                // new table.  Otherwise we lost the CAS to another racing put.
                // Simply retry from the start.
                if (V instanceof Prime)
                    return copy_slot_and_check(idx, expVal).putIfMatch(key, putval, expVal);
            }
        }

        // --- tableFull ---------------------------------------------------------
        // Heuristic to decide if this table is too full, and we should start a
        // new table.  Note that if a 'get' call has reprobed too many times and
        // decided the table must be full, then always the estimate_sum must be
        // high and we must report the table is full.  If we do not, then we might
        // end up deciding that the table is not full and inserting into the
        // current table, while a 'get' has decided the same key cannot be in this
        // table because of too many reprobes.  The invariant is:
        //   slots.estimate_sum >= max_reprobe_cnt >= reprobe_limit(len)

        private final boolean tableFull(int reprobe_cnt, int len) {
            return
                    // Do the cheap check first: we allow some number of reprobes always
                    reprobe_cnt >= REPROBE_LIMIT &&
                            // More expensive check: see if the table is > 1/4 full.
                            _slots.estimate_get() >= reprobe_limit(len);
        }

        // --- resize ------------------------------------------------------------
        // Resizing after too many probes.  "How Big???" heuristics are here.
        // Callers will (not this routine) will 'help_copy' any in-progress copy.
        // Since this routine has a fast cutout for copy-already-started, callers
        // MUST 'help_copy' lest we have a path which forever runs through
        // 'resize' only to discover a copy-in-progress which never progresses.

        private final CHM resize() {
            // Check for resize already in progress, probably triggered by another thread
            CHM newchm = _newchm;     // VOLATILE READ
            if (newchm != null)      // See if resize is already in progress
                return newchm;          // Use the new table already

            // No copy in-progress, so start one.  First up: compute new table size.
            int oldlen = _keys.length; // Old count of K,V pairs allowed
            int sz = size();          // Get current table count of active K,V pairs
            int newsz = sz;           // First size estimate

            // Heuristic to determine new size.  We expect plenty of dead-slots-with-keys
            // and we need some decent padding to avoid endless reprobing.
            if (_nbhml._opt_for_space) {
                // This heuristic leads to a much denser table with a higher reprobe rate
                if (sz >= (oldlen >> 1)) // If we are >50% full of keys then...
                    newsz = oldlen << 1;    // Double size
            } else {
                if (sz >= (oldlen >> 2)) { // If we are >25% full of keys then...
                    newsz = oldlen << 1;      // Double size
                    if (sz >= (oldlen >> 1)) // If we are >50% full of keys then...
                        newsz = oldlen << 2;    // Double double size
                }
            }

            // Last (re)size operation was very recent?  Then double again; slows
            // down resize operations for tables subject to a high key churn rate.
            long tm = System.currentTimeMillis();
            long q = 0;
            if (newsz <= oldlen &&    // New table would shrink or hold steady?
                    tm <= _nbhml._last_resize_milli + 10000 && // Recent resize (less than 1 sec ago)
                    //(q=_slots.estimate_sum()) >= (sz<<1) ) // 1/2 of keys are dead?
                    true)
                newsz = oldlen << 1;      // Double the existing size

            // Do not shrink, ever
            if (newsz < oldlen) newsz = oldlen;
            //System.out.println("old="+oldlen+" new="+newsz+" size()="+sz+" est_slots()="+q+" millis="+(tm-_nbhml._last_resize_milli));

            // Convert to power-of-2
            int log2;
            for (log2 = MIN_SIZE_LOG; (1 << log2) < newsz; log2++) ; // Compute log2 of size

            // Now limit the number of threads actually allocating memory to a
            // handful - lest we have 750 threads all trying to allocate a giant
            // resized array.
            long r = _resizers;
            while (!_resizerUpdater.compareAndSet(this, r, r + 1))
                r = _resizers;
            // Size calculation: 2 words (K+V) per table entry, plus a handful.  We
            // guess at 32-bit pointers; 64-bit pointers screws up the size calc by
            // 2x but does not screw up the heuristic very much.
            int megs = ((((1 << log2) << 1) + 4) << 3/*word to bytes*/) >> 20/*megs*/;
            if (r >= 2 && megs > 0) { // Already 2 guys trying; wait and see
                newchm = _newchm;        // Between dorking around, another thread did it
                if (newchm != null)     // See if resize is already in progress
                    return newchm;         // Use the new table already
                // TODO - use a wait with timeout, so we'll wakeup as soon as the new table
                // is ready, or after the timeout in any case.
                //synchronized( this ) { wait(8*megs); }         // Timeout - we always wakeup
                // For now, sleep a tad and see if the 2 guys already trying to make
                // the table actually get around to making it happen.
                try {
                    Thread.sleep(8 * megs);
                } catch (Exception e) {
                }
            }
            // Last check, since the 'new' below is expensive and there is a chance
            // that another thread slipped in a new thread while we ran the heuristic.
            newchm = _newchm;
            if (newchm != null)      // See if resize is already in progress
                return newchm;          // Use the new table already

            // New CHM - actually allocate the big arrays
            newchm = new CHM(_nbhml, _size, log2);

            // Another check after the slow allocation
            if (_newchm != null)     // See if resize is already in progress
                return _newchm;         // Use the new table already

            // The new table must be CAS'd in so only 1 winner amongst duplicate
            // racing resizing threads.  Extra CHM's will be GC'd.
            if (CAS_newchm(newchm)) { // NOW a resize-is-in-progress!
                //notifyAll();            // Wake up any sleepers
                //long nano = System.nanoTime();
                //System.out.println(" "+nano+" Resize from "+oldlen+" to "+(1<<log2)+" and had "+(_resizers-1)+" extras" );
                //System.out.print("["+log2);
            } else                    // CAS failed?
                newchm = _newchm;       // Reread new table
            return newchm;
        }


        // The next part of the table to copy.  It monotonically transits from zero
        // to _keys.length.  Visitors to the table can claim 'work chunks' by
        // CAS'ing this field up, then copying the indicated indices from the old
        // table to the new table.  Workers are not required to finish any chunk;
        // the counter simply wraps and work is copied duplicately until somebody
        // somewhere completes the count.
        volatile long _copyIdx = 0;
        static private final AtomicLongFieldUpdater<CHM> _copyIdxUpdater =
                AtomicLongFieldUpdater.newUpdater(CHM.class, "_copyIdx");

        // Work-done reporting.  Used to efficiently signal when we can move to
        // the new table.  From 0 to len(oldkvs) refers to copying from the old
        // table to the new.
        volatile long _copyDone = 0;
        static private final AtomicLongFieldUpdater<CHM> _copyDoneUpdater =
                AtomicLongFieldUpdater.newUpdater(CHM.class, "_copyDone");

        // --- help_copy_impl ----------------------------------------------------
        // Help along an existing resize operation.  We hope its the top-level
        // copy (it was when we started) but this CHM might have been promoted out
        // of the top position.

        private final void help_copy_impl(final boolean copy_all) {
            final CHM newchm = _newchm;
            assert newchm != null;    // Already checked by caller
            int oldlen = _keys.length; // Total amount to copy
            final int MIN_COPY_WORK = Math.min(oldlen, 1024); // Limit per-thread work

            // ---
            int panic_start = -1;
            int copyidx = -9999;            // Fool javac to think it's initialized
            while (_copyDone < oldlen) { // Still needing to copy?
                // Carve out a chunk of work.  The counter wraps around so every
                // thread eventually tries to copy every slot repeatedly.

                // We "panic" if we have tried TWICE to copy every slot - and it still
                // has not happened.  i.e., twice some thread somewhere claimed they
                // would copy 'slot X' (by bumping _copyIdx) but they never claimed to
                // have finished (by bumping _copyDone).  Our choices become limited:
                // we can wait for the work-claimers to finish (and become a blocking
                // algorithm) or do the copy work ourselves.  Tiny tables with huge
                // thread counts trying to copy the table often 'panic'.
                if (panic_start == -1) { // No panic?
                    copyidx = (int) _copyIdx;
                    while (copyidx < (oldlen << 1) && // 'panic' check
                            !_copyIdxUpdater.compareAndSet(this, copyidx, copyidx + MIN_COPY_WORK))
                        copyidx = (int) _copyIdx;     // Re-read
                    if (!(copyidx < (oldlen << 1))) // Panic!
                        panic_start = copyidx;       // Record where we started to panic-copy
                }

                // We now know what to copy.  Try to copy.
                int workdone = 0;
                for (int i = 0; i < MIN_COPY_WORK; i++)
                    if (copy_slot((copyidx + i) & (oldlen - 1))) // Made an oldtable slot go dead?
                        workdone++;         // Yes!
                if (workdone > 0)      // Report work-done occasionally
                    copy_check_and_promote(workdone);// See if we can promote
                //for( int i=0; i<MIN_COPY_WORK; i++ )
                //  if( copy_slot((copyidx+i)&(oldlen-1)) ) // Made an oldtable slot go dead?
                //    copy_check_and_promote( 1 );// See if we can promote

                copyidx += MIN_COPY_WORK;
                // Uncomment these next 2 lines to turn on incremental table-copy.
                // Otherwise this thread continues to copy until it is all done.
                if (!copy_all && panic_start == -1) // No panic?
                    return;               // Then done copying after doing MIN_COPY_WORK
            }
            // Extra promotion check, in case another thread finished all copying
            // then got stalled before promoting.
            copy_check_and_promote(0); // See if we can promote
        }


        // --- copy_slot_and_check -----------------------------------------------
        // Copy slot 'idx' from the old table to the new table.  If this thread
        // confirmed the copy, update the counters and check for promotion.
        //
        // Returns the result of reading the volatile _newchm, mostly as a
        // convenience to callers.  We come here with 1-shot copy requests
        // typically because the caller has found a Prime, and has not yet read
        // the _newchm volatile - which must have changed from null-to-not-null
        // before any Prime appears.  So the caller needs to read the _newchm
        // field to retry his operation in the new table, but probably has not
        // read it yet.

        private final CHM copy_slot_and_check(int idx, Object should_help) {
            // We're only here because the caller saw a Prime, which implies a
            // table-copy is in progress.
            assert _newchm != null;
            if (copy_slot(idx))      // Copy the desired slot
                copy_check_and_promote(1); // Record the slot copied
            // Generically help along any copy (except if called recursively from a helper)
            if (should_help != null) _nbhml.help_copy();
            return _newchm;
        }

        // --- copy_check_and_promote --------------------------------------------

        private final void copy_check_and_promote(int workdone) {
            int oldlen = _keys.length;
            // We made a slot unusable and so did some of the needed copy work
            long copyDone = _copyDone;
            long nowDone = copyDone + workdone;
            assert nowDone <= oldlen;
            if (workdone > 0) {
                while (!_copyDoneUpdater.compareAndSet(this, copyDone, nowDone)) {
                    copyDone = _copyDone;   // Reload, retry
                    nowDone = copyDone + workdone;
                    assert nowDone <= oldlen;
                }
                //if( (10*copyDone/oldlen) != (10*nowDone/oldlen) )
                //  System.out.print(" "+nowDone*100/oldlen+"%"+"_"+(_copyIdx*100/oldlen)+"%");
            }

            // Check for copy being ALL done, and promote.  Note that we might have
            // nested in-progress copies and manage to finish a nested copy before
            // finishing the top-level copy.  We only promote top-level copies.
            if (nowDone == oldlen &&   // Ready to promote this table?
                    _nbhml._chm == this && // Looking at the top-level table?
                    // Attempt to promote
                    _nbhml.CAS(_chm_offset, this, _newchm)) {
                _nbhml._last_resize_milli = System.currentTimeMillis();  // Record resize time for next check
                //long nano = System.nanoTime();
                //System.out.println(" "+nano+" Promote table "+oldlen+" to "+_newchm._keys.length);
                //System.out.print("_"+oldlen+"]");
            }
        }

        // --- copy_slot ---------------------------------------------------------
        // Copy one K/V pair from oldkvs[i] to newkvs.  Returns true if we can
        // confirm that the new table guaranteed has a value for this old-table
        // slot.  We need an accurate confirmed-copy count so that we know when we
        // can promote (if we promote the new table too soon, other threads may
        // 'miss' on values not-yet-copied from the old table).  We don't allow
        // any direct updates on the new table, unless they first happened to the
        // old table - so that any transition in the new table from null to
        // not-null must have been from a copy_slot (or other old-table overwrite)
        // and not from a thread directly writing in the new table.  Thus we can
        // count null-to-not-null transitions in the new table.

        private boolean copy_slot(int idx) {
            // Blindly set the key slot from NO_KEY to some key which hashes here,
            // to eagerly stop fresh put's from inserting new values in the old
            // table when the old table is mid-resize.  We don't need to act on the
            // results here, because our correctness stems from box'ing the Value
            // field.  Slamming the Key field is a minor speed optimization.
            long key;
            while ((key = _keys[idx]) == NO_KEY)
                CAS_key(idx, NO_KEY, (idx + _keys.length)/*a non-zero key which hashes here*/);

            // ---
            // Prevent new values from appearing in the old table.
            // Box what we see in the old table, to prevent further updates.
            Object oldval = _vals[idx]; // Read OLD table
            while (!(oldval instanceof Prime)) {
                final Prime box = (oldval == null || oldval == TOMBSTONE) ? TOMBPRIME : new Prime(oldval);
                if (CAS_val(idx, oldval, box)) { // CAS down a box'd version of oldval
                    // If we made the Value slot hold a TOMBPRIME, then we both
                    // prevented further updates here but also the (absent) oldval is
                    // vaccuously available in the new table.  We return with true here:
                    // any thread looking for a value for this key can correctly go
                    // straight to the new table and skip looking in the old table.
                    if (box == TOMBPRIME)
                        return true;
                    // Otherwise we boxed something, but it still needs to be
                    // copied into the new table.
                    oldval = box;         // Record updated oldval
                    break;                // Break loop; oldval is now boxed by us
                }
                oldval = _vals[idx];    // Else try, try again
            }
            if (oldval == TOMBPRIME) return false; // Copy already complete here!

            // ---
            // Copy the value into the new table, but only if we overwrite a null.
            // If another value is already in the new table, then somebody else
            // wrote something there and that write is happens-after any value that
            // appears in the old table.  If putIfMatch does not find a null in the
            // new table - somebody else should have recorded the null-not_null
            // transition in this copy.
            Object old_unboxed = ((Prime) oldval)._V;
            assert old_unboxed != TOMBSTONE;
            boolean copied_into_new = (_newchm.putIfMatch(key, old_unboxed, null) == null);

            // ---
            // Finally, now that any old value is exposed in the new table, we can
            // forever hide the old-table value by slapping a TOMBPRIME down.  This
            // will stop other threads from uselessly attempting to copy this slot
            // (i.e., it's a speed optimization not a correctness issue).
            while (!CAS_val(idx, oldval, TOMBPRIME))
                oldval = _vals[idx];

            return copied_into_new;
        } // end copy_slot
    } // End of CHM


    // --- Snapshot ------------------------------------------------------------

    private class SnapshotV implements Iterator<TypeV>, Enumeration<TypeV> {
        final CHM _sschm;

        public SnapshotV() {
            CHM topchm;
            while (true) {           // Verify no table-copy-in-progress
                topchm = _chm;
                if (topchm._newchm == null) // No table-copy-in-progress
                    break;
                // Table copy in-progress - so we cannot get a clean iteration.  We
                // must help finish the table copy before we can start iterating.
                topchm.help_copy_impl(true);
            }
            // The "linearization point" for the iteration.  Every key in this table
            // will be visited, but keys added later might be skipped or even be
            // added to a following table (also not iterated over).
            _sschm = topchm;
            // Warm-up the iterator
            _idx = -1;
            next();
        }

        int length() {
            return _sschm._keys.length;
        }

        long key(final int idx) {
            return _sschm._keys[idx];
        }

        private int _idx;           // -2 for NO_KEY, -1 for CHECK_NEW_TABLE_LONG, 0-keys.length
        private long _nextK, _prevK; // Last 2 keys found
        private TypeV _nextV, _prevV; // Last 2 values found

        public boolean hasNext() {
            return _nextV != null;
        }

        public TypeV next() {
            // 'next' actually knows what the next value will be - it had to
            // figure that out last go 'round lest 'hasNext' report true and
            // some other thread deleted the last value.  Instead, 'next'
            // spends all its effort finding the key that comes after the
            // 'next' key.
            if (_idx != -1 && _nextV == null) throw new NoSuchElementException();
            _prevK = _nextK;          // This will become the previous key
            _prevV = _nextV;          // This will become the previous value
            _nextV = null;            // We have no more next-key
            // Attempt to set <_nextK,_nextV> to the next K,V pair.
            // _nextV is the trigger: stop searching when it is != null
            if (_idx == -1) {        // Check for NO_KEY
                _idx = 0;               // Setup for next phase of search
                _nextK = NO_KEY;
                if ((_nextV = get(_nextK)) != null) return _prevV;
            }
            while (_idx < length()) {  // Scan array
                _nextK = key(_idx++); // Get a key that definitely is in the set (for the moment!)
                if (_nextK != NO_KEY && // Found something?
                        (_nextV = get(_nextK)) != null)
                    break;                // Got it!  _nextK is a valid Key
            }                         // Else keep scanning
            return _prevV;            // Return current value.
        }

        public void remove() {
            if (_prevV == null) throw new IllegalStateException();
            _sschm.putIfMatch(_prevK, TOMBSTONE, _prevV);
            _prevV = null;
        }

        public TypeV nextElement() {
            return next();
        }

        public boolean hasMoreElements() {
            return hasNext();
        }
    }

    /**
     * Returns an enumeration of the values in this table.
     *
     * @return an enumeration of the values in this table
     * @see #values()
     */
    public Enumeration<TypeV> elements() {
        return new SnapshotV();
    }

    // --- values --------------------------------------------------------------

    /**
     * Returns a {@link Collection} view of the values contained in this map.
     * The collection is backed by the map, so changes to the map are reflected
     * in the collection, and vice-versa.  The collection supports element
     * removal, which removes the corresponding mapping from this map, via the
     * <tt>Iterator.remove</tt>, <tt>Collection.remove</tt>,
     * <tt>removeAll</tt>, <tt>retainAll</tt>, and <tt>clear</tt> operations.
     * It does not support the <tt>add</tt> or <tt>addAll</tt> operations.
     * <p/>
     * <p>The view's <tt>iterator</tt> is a "weakly consistent" iterator that
     * will never throw {@link ConcurrentModificationException}, and guarantees
     * to traverse elements as they existed upon construction of the iterator,
     * and may (but is not guaranteed to) reflect any modifications subsequent
     * to construction.
     */
    public Collection<TypeV> values() {
        return new AbstractCollection<TypeV>() {
            public void clear() {
                NonBlockingHashMapLong.this.clear();
            }

            public int size() {
                return NonBlockingHashMapLong.this.size();
            }

            public boolean contains(Object v) {
                return NonBlockingHashMapLong.this.containsValue(v);
            }

            public Iterator<TypeV> iterator() {
                return new SnapshotV();
            }
        };
    }

    // --- keySet --------------------------------------------------------------

    /**
     * A class which implements the {@link Iterator} and {@link Enumeration}
     * interfaces, generified to the {@link Long} class and supporting a
     * <strong>non-auto-boxing</strong> {@link #nextLong} function.
     */
    public class IteratorLong implements Iterator<Long>, Enumeration<Long> {
        private final SnapshotV _ss;

        /**
         * A new IteratorLong
         */
        public IteratorLong() {
            _ss = new SnapshotV();
        }

        /**
         * Remove last key returned by {@link #next} or {@link #nextLong}.
         */
        public void remove() {
            _ss.remove();
        }

        /**
         * <strong>Auto-box</strong> and return the next key.
         */
        public Long next() {
            _ss.next();
            return _ss._prevK;
        }

        /**
         * Return the next key as a primitive {@code long}.
         */
        public long nextLong() {
            _ss.next();
            return _ss._prevK;
        }

        /**
         * True if there are more keys to iterate over.
         */
        public boolean hasNext() {
            return _ss.hasNext();
        }

        /**
         * <strong>Auto-box</strong> and return the next key.
         */
        public Long nextElement() {
            return next();
        }

        /**
         * True if there are more keys to iterate over.
         */
        public boolean hasMoreElements() {
            return hasNext();
        }
    }

    /**
     * Returns an enumeration of the <strong>auto-boxed</strong> keys in this table.
     * <strong>Warning:</strong> this version will auto-box all returned keys.
     *
     * @return an enumeration of the auto-boxed keys in this table
     * @see #keySet()
     */
    public Enumeration<Long> keys() {
        return new IteratorLong();
    }

    /**
     * Returns a {@link Set} view of the keys contained in this map; with care
     * the keys may be iterated over <strong>without auto-boxing</strong>.  The
     * set is backed by the map, so changes to the map are reflected in the
     * set, and vice-versa.  The set supports element removal, which removes
     * the corresponding mapping from this map, via the
     * <tt>Iterator.remove</tt>, <tt>Set.remove</tt>, <tt>removeAll</tt>,
     * <tt>retainAll</tt>, and <tt>clear</tt> operations.  It does not support
     * the <tt>add</tt> or <tt>addAll</tt> operations.
     * <p/>
     * <p>The view's <tt>iterator</tt> is a "weakly consistent" iterator that
     * will never throw {@link ConcurrentModificationException}, and guarantees
     * to traverse elements as they existed upon construction of the iterator,
     * and may (but is not guaranteed to) reflect any modifications subsequent
     * to construction.
     */
    public Set<Long> keySet() {
        return new AbstractSet<Long>() {
            public void clear() {
                NonBlockingHashMapLong.this.clear();
            }

            public int size() {
                return NonBlockingHashMapLong.this.size();
            }

            public boolean contains(Object k) {
                return NonBlockingHashMapLong.this.containsKey(k);
            }

            public boolean remove(Object k) {
                return NonBlockingHashMapLong.this.remove(k) != null;
            }

            public IteratorLong iterator() {
                return new IteratorLong();
            }
        };
    }


    // --- entrySet ------------------------------------------------------------
    // Warning: Each call to 'next' in this iterator constructs a new Long and a
    // new NBHMLEntry.

    private class NBHMLEntry extends AbstractEntry<Long, TypeV> {
        NBHMLEntry(final Long k, final TypeV v) {
            super(k, v);
        }

        public TypeV setValue(final TypeV val) {
            if (val == null) throw new NullPointerException();
            _val = val;
            return put(_key, val);
        }
    }

    private class SnapshotE implements Iterator<Map.Entry<Long, TypeV>> {
        final SnapshotV _ss;

        public SnapshotE() {
            _ss = new SnapshotV();
        }

        public void remove() {
            _ss.remove();
        }

        public Map.Entry<Long, TypeV> next() {
            _ss.next();
            return new NBHMLEntry(_ss._prevK, _ss._prevV);
        }

        public boolean hasNext() {
            return _ss.hasNext();
        }
    }

    /**
     * Returns a {@link Set} view of the mappings contained in this map.  The
     * set is backed by the map, so changes to the map are reflected in the
     * set, and vice-versa.  The set supports element removal, which removes
     * the corresponding mapping from the map, via the
     * <tt>Iterator.remove</tt>, <tt>Set.remove</tt>, <tt>removeAll</tt>,
     * <tt>retainAll</tt>, and <tt>clear</tt> operations.  It does not support
     * the <tt>add</tt> or <tt>addAll</tt> operations.
     * <p/>
     * <p>The view's <tt>iterator</tt> is a "weakly consistent" iterator
     * that will never throw {@link ConcurrentModificationException},
     * and guarantees to traverse elements as they existed upon
     * construction of the iterator, and may (but is not guaranteed to)
     * reflect any modifications subsequent to construction.
     * <p/>
     * <p><strong>Warning:</strong> the iterator associated with this Set
     * requires the creation of {@link java.util.Map.Entry} objects with each
     * iteration.  The {@link org.cliffc.high_scale_lib.NonBlockingHashMap}
     * does not normally create or using {@link java.util.Map.Entry} objects so
     * they will be created soley to support this iteration.  Iterating using
     * {@link #keySet} or {@link #values} will be more efficient.  In addition,
     * this version requires <strong>auto-boxing</strong> the keys.
     */
    public Set<Map.Entry<Long, TypeV>> entrySet() {
        return new AbstractSet<Map.Entry<Long, TypeV>>() {
            public void clear() {
                NonBlockingHashMapLong.this.clear();
            }

            public int size() {
                return NonBlockingHashMapLong.this.size();
            }

            public boolean remove(final Object o) {
                if (!(o instanceof Map.Entry)) return false;
                final Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
                return NonBlockingHashMapLong.this.remove(e.getKey(), e.getValue());
            }

            public boolean contains(final Object o) {
                if (!(o instanceof Map.Entry)) return false;
                final Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
                TypeV v = get(e.getKey());
                return v.equals(e.getValue());
            }

            public Iterator<Map.Entry<Long, TypeV>> iterator() {
                return new SnapshotE();
            }
        };
    }

    // --- writeObject -------------------------------------------------------
    // Write a NBHML to a stream

    private void writeObject(java.io.ObjectOutputStream s) throws IOException {
        s.defaultWriteObject();     // Write nothing
        for (long K : keySet()) {
            final Object V = get(K);  // Do an official 'get'
            s.writeLong(K);         // Write the <long,TypeV> pair
            s.writeObject(V);
        }
        s.writeLong(NO_KEY);        // Sentinel to indicate end-of-data
        s.writeObject(null);
    }

    // --- readObject --------------------------------------------------------
    // Read a CHM from a stream

    private void readObject(java.io.ObjectInputStream s) throws IOException, ClassNotFoundException {
        s.defaultReadObject();      // Read nothing
        initialize(MIN_SIZE);
        for (; ;) {
            final long K = s.readLong();
            final TypeV V = (TypeV) s.readObject();
            if (K == NO_KEY && V == null) break;
            put(K, V);               // Insert with an offical put
        }
    }

}  // End NonBlockingHashMapLong class
