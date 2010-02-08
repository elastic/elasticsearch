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

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * A lock-free alternate implementation of {@link java.util.concurrent.ConcurrentHashMap}
 * with better scaling properties and generally lower costs to mutate the Map.
 * It provides identical correctness properties as ConcurrentHashMap.  All
 * operations are non-blocking and multi-thread safe, including all update
 * operations.  {@link NonBlockingHashMap} scales substatially better than
 * {@link java.util.concurrent.ConcurrentHashMap} for high update rates, even with a
 * large concurrency factor.  Scaling is linear up to 768 CPUs on a 768-CPU
 * Azul box, even with 100% updates or 100% reads or any fraction in-between.
 * Linear scaling up to all cpus has been observed on a 32-way Sun US2 box,
 * 32-way Sun Niagra box, 8-way Intel box and a 4-way Power box.
 * <p/>
 * This class obeys the same functional specification as {@link
 * java.util.Hashtable}, and includes versions of methods corresponding to
 * each method of <tt>Hashtable</tt>. However, even though all operations are
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
 * does <em>not</em> allow <tt>null</tt> to be used as a key or value.
 *
 * @author Cliff Click
 * @author Prashant Deva
 *         Modified from original NonBlockingHashMap to use identity equality.
 *         Uses System.identityHashCode() to calculate hashMap.
 *         Key equality is compared using '=='.
 * @param <TypeK> the type of keys maintained by this map
 * @param <TypeV> the type of mapped values
 * @since 1.5
 */

public class NonBlockingIdentityHashMap<TypeK, TypeV>
        extends AbstractMap<TypeK, TypeV>
        implements ConcurrentMap<TypeK, TypeV>, Cloneable, Serializable {

    private static final long serialVersionUID = 1234123412341234123L;

    private static final int REPROBE_LIMIT = 10; // Too many reprobes then force a table-resize

    // --- Bits to allow Unsafe access to arrays
    private static final Unsafe _unsafe = UtilUnsafe.getUnsafe();
    private static final int _Obase = _unsafe.arrayBaseOffset(Object[].class);
    private static final int _Oscale = _unsafe.arrayIndexScale(Object[].class);

    private static long rawIndex(final Object[] ary, final int idx) {
        assert idx >= 0 && idx < ary.length;
        return _Obase + idx * _Oscale;
    }

    // --- Setup to use Unsafe
    private static final long _kvs_offset;

    static {                      // <clinit>
        Field f = null;
        try {
            f = NonBlockingHashMap.class.getDeclaredField("_kvs");
        }
        catch (java.lang.NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
        _kvs_offset = _unsafe.objectFieldOffset(f);
    }

    private final boolean CAS_kvs(final Object[] oldkvs, final Object[] newkvs) {
        return _unsafe.compareAndSwapObject(this, _kvs_offset, oldkvs, newkvs);
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

    // --- hash ----------------------------------------------------------------
    // Helper function to spread lousy hashCodes

    private static final int hash(final Object key) {
        int h = System.identityHashCode(key); // The real hashCode call
        h ^= (h >>> 20) ^ (h >>> 12);
        h ^= (h >>> 7) ^ (h >>> 4);
        return h;
    }

    // --- The Hash Table --------------------
    // Slot 0 is always used for a 'CHM' entry below to hold the interesting
    // bits of the hash table.  Slot 1 holds full hashes as an array of ints.
    // Slots {2,3}, {4,5}, etc hold {Key,Value} pairs.  The entire hash table
    // can be atomically replaced by CASing the _kvs field.
    //
    // Why is CHM buried inside the _kvs Object array, instead of the other way
    // around?  The CHM info is used during resize events and updates, but not
    // during standard 'get' operations.  I assume 'get' is much more frequent
    // than 'put'.  'get' can skip the extra indirection of skipping through the
    // CHM to reach the _kvs array.
    private transient Object[] _kvs;

    private static final CHM chm(Object[] kvs) {
        return (CHM) kvs[0];
    }

    private static final int[] hashes(Object[] kvs) {
        return (int[]) kvs[1];
    }

    // Number of K,V pairs in the table

    private static final int len(Object[] kvs) {
        return (kvs.length - 2) >> 1;
    }

    // Time since last resize
    private transient long _last_resize_milli;

    // --- Minimum table size ----------------
    // Pick size 8 K/V pairs, which turns into (8*2+2)*4+12 = 84 bytes on a
    // standard 32-bit HotSpot, and (8*2+2)*8+12 = 156 bytes on 64-bit Azul.
    private static final int MIN_SIZE_LOG = 3;             //
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

    // --- key,val -------------------------------------------------------------
    // Access K,V for a given idx
    //
    // Note that these are static, so that the caller is forced to read the _kvs
    // field only once, and share that read across all key/val calls - lest the
    // _kvs field move out from under us and back-to-back key & val calls refer
    // to different _kvs arrays.

    private static final Object key(Object[] kvs, int idx) {
        return kvs[(idx << 1) + 2];
    }

    private static final Object val(Object[] kvs, int idx) {
        return kvs[(idx << 1) + 3];
    }

    private static final boolean CAS_key(Object[] kvs, int idx, Object old, Object key) {
        return _unsafe.compareAndSwapObject(kvs, rawIndex(kvs, (idx << 1) + 2), old, key);
    }

    private static final boolean CAS_val(Object[] kvs, int idx, Object old, Object val) {
        return _unsafe.compareAndSwapObject(kvs, rawIndex(kvs, (idx << 1) + 3), old, val);
    }


    // --- dump ----------------------------------------------------------------

    /**
     * Verbose printout of table internals, useful for debugging.
     */
    public final void print() {
        System.out.println("=========");
        print2(_kvs);
        System.out.println("=========");
    }

    // print the entire state of the table

    private final void print(Object[] kvs) {
        for (int i = 0; i < len(kvs); i++) {
            Object K = key(kvs, i);
            if (K != null) {
                String KS = (K == TOMBSTONE) ? "XXX" : K.toString();
                Object V = val(kvs, i);
                Object U = Prime.unbox(V);
                String p = (V == U) ? "" : "prime_";
                String US = (U == TOMBSTONE) ? "tombstone" : U.toString();
                System.out.println("" + i + " (" + KS + "," + p + US + ")");
            }
        }
        Object[] newkvs = chm(kvs)._newkvs; // New table, if any
        if (newkvs != null) {
            System.out.println("----");
            print(newkvs);
        }
    }

    // print only the live values, broken down by the table they are in

    private final void print2(Object[] kvs) {
        for (int i = 0; i < len(kvs); i++) {
            Object key = key(kvs, i);
            Object val = val(kvs, i);
            Object U = Prime.unbox(val);
            if (key != null && key != TOMBSTONE &&  // key is sane
                    val != null && U != TOMBSTONE) { // val is sane
                String p = (val == U) ? "" : "prime_";
                System.out.println("" + i + " (" + key + "," + p + val + ")");
            }
        }
        Object[] newkvs = chm(kvs)._newkvs; // New table, if any
        if (newkvs != null) {
            System.out.println("----");
            print2(newkvs);
        }
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

    // --- NonBlockingHashMap --------------------------------------------------
    // Constructors

    /**
     * Create a new NonBlockingHashMap with default minimum size (currently set
     * to 8 K/V pairs or roughly 84 bytes on a standard 32-bit JVM).
     */
    public NonBlockingIdentityHashMap() {
        this(MIN_SIZE);
    }

    /**
     * Create a new NonBlockingHashMap with initial room for the given number of
     * elements, thus avoiding internal resizing operations to reach an
     * appropriate size.  Large numbers here when used with a small count of
     * elements will sacrifice space for a small amount of time gained.  The
     * initial size will be rounded up internally to the next larger power of 2.
     */
    public NonBlockingIdentityHashMap(final int initial_sz) {
        initialize(initial_sz);
    }

    private final void initialize(int initial_sz) {
        if (initial_sz < 0) throw new IllegalArgumentException();
        int i;                      // Convert to next largest power-of-2
        if (initial_sz > 1024 * 1024) initial_sz = 1024 * 1024;
        for (i = MIN_SIZE_LOG; (1 << i) < (initial_sz << 2); i++) ;
        // Double size for K,V pairs, add 1 for CHM and 1 for hashes
        _kvs = new Object[((1 << i) << 1) + 2];
        _kvs[0] = new CHM(new Counter()); // CHM in slot 0
        _kvs[1] = new int[1 << i];          // Matching hash entries
        _last_resize_milli = System.currentTimeMillis();
    }

    // Version for subclassed readObject calls, to be called after the defaultReadObject

    protected final void initialize() {
        initialize(MIN_SIZE);
    }

    // --- wrappers ------------------------------------------------------------

    /**
     * Returns the number of key-value mappings in this map.
     *
     * @return the number of key-value mappings in this map
     */
    @Override
    public int size() {
        return chm(_kvs).size();
    }

    /**
     * Returns <tt>size() == 0</tt>.
     *
     * @return <tt>size() == 0</tt>
     */
    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Tests if the key in the table using the <tt>equals</tt> method.
     *
     * @return <tt>true</tt> if the key is in the table using the <tt>equals</tt> method
     * @throws NullPointerException if the specified key is null
     */
    @Override
    public boolean containsKey(Object key) {
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
     * Maps the specified key to the specified value in the table.  Neither key
     * nor value can be null.
     * <p> The value can be retrieved by calling {@link #get} with a key that is
     * equal to the original key.
     *
     * @param key key with which the specified value is to be associated
     * @param val value to be associated with the specified key
     * @return the previous value associated with <tt>key</tt>, or
     *         <tt>null</tt> if there was no mapping for <tt>key</tt>
     * @throws NullPointerException if the specified key or value is null
     */
    @Override
    public TypeV put(TypeK key, TypeV val) {
        return putIfMatch(key, val, NO_MATCH_OLD);
    }

    /**
     * Atomically, do a {@link #put} if-and-only-if the key is not mapped.
     * Useful to ensure that only a single mapping for the key exists, even if
     * many threads are trying to create the mapping in parallel.
     *
     * @return the previous value associated with the specified key,
     *         or <tt>null</tt> if there was no mapping for the key
     * @throws NullPointerException if the specified key or value is null
     */
    public TypeV putIfAbsent(TypeK key, TypeV val) {
        return putIfMatch(key, val, TOMBSTONE);
    }

    /**
     * Removes the key (and its corresponding value) from this map.
     * This method does nothing if the key is not in the map.
     *
     * @return the previous value associated with <tt>key</tt>, or
     *         <tt>null</tt> if there was no mapping for <tt>key</tt>
     * @throws NullPointerException if the specified key is null
     */
    @Override
    public TypeV remove(Object key) {
        return putIfMatch(key, TOMBSTONE, NO_MATCH_OLD);
    }

    /**
     * Atomically do a {@link #remove(Object)} if-and-only-if the key is mapped
     * to a value which is <code>equals</code> to the given value.
     *
     * @throws NullPointerException if the specified key or value is null
     */
    public boolean remove(Object key, Object val) {
        return putIfMatch(key, TOMBSTONE, val) == val;
    }

    /**
     * Atomically do a <code>put(key,val)</code> if-and-only-if the key is
     * mapped to some value already.
     *
     * @throws NullPointerException if the specified key or value is null
     */
    public TypeV replace(TypeK key, TypeV val) {
        return putIfMatch(key, val, MATCH_ANY);
    }

    /**
     * Atomically do a <code>put(key,newValue)</code> if-and-only-if the key is
     * mapped a value which is <code>equals</code> to <code>oldValue</code>.
     *
     * @throws NullPointerException if the specified key or value is null
     */
    public boolean replace(TypeK key, TypeV oldValue, TypeV newValue) {
        return putIfMatch(key, newValue, oldValue) == oldValue;
    }

    private final TypeV putIfMatch(Object key, Object newVal, Object oldVal) {
        if (oldVal == null || newVal == null) throw new NullPointerException();
        final Object res = putIfMatch(this, _kvs, key, newVal, oldVal);
        assert !(res instanceof Prime);
        assert res != null;
        return res == TOMBSTONE ? null : (TypeV) res;
    }


    /**
     * Copies all of the mappings from the specified map to this one, replacing
     * any existing mappings.
     *
     * @param m mappings to be stored in this map
     */
    @Override
    public void putAll(Map<? extends TypeK, ? extends TypeV> m) {
        for (Map.Entry<? extends TypeK, ? extends TypeV> e : m.entrySet())
            put(e.getKey(), e.getValue());
    }

    /**
     * Removes all of the mappings from this map.
     */
    @Override
    public void clear() {         // Smack a new empty table down
        Object[] newkvs = new NonBlockingIdentityHashMap(MIN_SIZE)._kvs;
        while (!CAS_kvs(_kvs, newkvs)) // Spin until the clear works
            ;
    }

    /**
     * Returns <tt>true</tt> if this Map maps one or more keys to the specified
     * value.  <em>Note</em>: This method requires a full internal traversal of the
     * hash table and is much slower than {@link #containsKey}.
     *
     * @param val value whose presence in this map is to be tested
     * @return <tt>true</tt> if this map maps one or more keys to the specified value
     * @throws NullPointerException if the specified value is null
     */
    @Override
    public boolean containsValue(final Object val) {
        if (val == null) throw new NullPointerException();
        for (TypeV V : values())
            if (V == val || V.equals(val))
                return true;
        return false;
    }

    // This function is supposed to do something for Hashtable, and the JCK
    // tests hang until it gets called... by somebody ... for some reason,
    // any reason....

    protected void rehash() {
    }

    /**
     * Creates a shallow copy of this hashtable. All the structure of the
     * hashtable itself is copied, but the keys and values are not cloned.
     * This is a relatively expensive operation.
     *
     * @return a clone of the hashtable.
     */
    @Override
    public Object clone() {
        try {
            // Must clone, to get the class right; NBHM might have been
            // extended so it would be wrong to just make a new NBHM.
            NonBlockingIdentityHashMap<TypeK, TypeV> t = (NonBlockingIdentityHashMap<TypeK, TypeV>) super.clone();
            // But I don't have an atomic clone operation - the underlying _kvs
            // structure is undergoing rapid change.  If I just clone the _kvs
            // field, the CHM in _kvs[0] won't be in sync.
            //
            // Wipe out the cloned array (it was shallow anyways).
            t.clear();
            // Now copy sanely
            for (TypeK K : keySet()) {
                final TypeV V = get(K);  // Do an official 'get'
                t.put(K, V);
            }
            return t;
        } catch (CloneNotSupportedException e) {
            // this shouldn't happen, since we are Cloneable
            throw new InternalError();
        }
    }

    /**
     * Returns a string representation of this map.  The string representation
     * consists of a list of key-value mappings in the order returned by the
     * map's <tt>entrySet</tt> view's iterator, enclosed in braces
     * (<tt>"{}"</tt>).  Adjacent mappings are separated by the characters
     * <tt>", "</tt> (comma and space).  Each key-value mapping is rendered as
     * the key followed by an equals sign (<tt>"="</tt>) followed by the
     * associated value.  Keys and values are converted to strings as by
     * {@link String#valueOf(Object)}.
     *
     * @return a string representation of this map
     */
    @Override
    public String toString() {
        Iterator<Entry<TypeK, TypeV>> i = entrySet().iterator();
        if (!i.hasNext())
            return "{}";

        StringBuilder sb = new StringBuilder();
        sb.append('{');
        for (; ;) {
            Entry<TypeK, TypeV> e = i.next();
            TypeK key = e.getKey();
            TypeV value = e.getValue();
            sb.append(key == this ? "(this Map)" : key);
            sb.append('=');
            sb.append(value == this ? "(this Map)" : value);
            if (!i.hasNext())
                return sb.append('}').toString();
            sb.append(", ");
        }
    }

    // --- get -----------------------------------------------------------------

    /**
     * Returns the value to which the specified key is mapped, or {@code null}
     * if this map contains no mapping for the key.
     * <p>More formally, if this map contains a mapping from a key {@code k} to
     * a value {@code v} such that {@code key.equals(k)}, then this method
     * returns {@code v}; otherwise it returns {@code null}.  (There can be at
     * most one such mapping.)
     *
     * @throws NullPointerException if the specified key is null
     */
    // Never returns a Prime nor a Tombstone.
    @Override
    public TypeV get(Object key) {
        final int fullhash = hash(key); // throws NullPointerException if key is null
        final Object V = get_impl(this, _kvs, key, fullhash);
        assert !(V instanceof Prime); // Never return a Prime
        return (TypeV) V;
    }

    private static final Object get_impl(final NonBlockingIdentityHashMap topmap, final Object[] kvs, final Object key, final int fullhash) {
        final int len = len(kvs); // Count of key/value pairs, reads kvs.length
        final CHM chm = chm(kvs); // The CHM, for a volatile read below; reads slot 0 of kvs

        int idx = fullhash & (len - 1); // First key hash

        // Main spin/reprobe loop, looking for a Key hit
        int reprobe_cnt = 0;
        while (true) {
            // Probe table.  Each read of 'val' probably misses in cache in a big
            // table; hopefully the read of 'key' then hits in cache.
            final Object K = key(kvs, idx); // Get key   before volatile read, could be null
            final Object V = val(kvs, idx); // Get value before volatile read, could be null or Tombstone or Prime
            if (K == null) return null;   // A clear miss

            // We need a volatile-read here to preserve happens-before semantics on
            // newly inserted Keys.  If the Key body was written just before inserting
            // into the table a Key-compare here might read the uninitalized Key body.
            // Annoyingly this means we have to volatile-read before EACH key compare.
            // .
            // We also need a volatile-read between reading a newly inserted Value
            // and returning the Value (so the user might end up reading the stale
            // Value contents).  Same problem as with keys - and the one volatile
            // read covers both.
            final Object[] newkvs = chm._newkvs; // VOLATILE READ before key compare

            // Key-compare
            if (K == key) {
                // Key hit!  Check for no table-copy-in-progress
                if (!(V instanceof Prime)) // No copy?
                    return (V == TOMBSTONE) ? null : V; // Return the value
                // Key hit - but slot is (possibly partially) copied to the new table.
                // Finish the copy & retry in the new table.
                return get_impl(topmap, chm.copy_slot_and_check(topmap, kvs, idx, key), key, fullhash); // Retry in the new table
            }
            // get and put must have the same key lookup logic!  But only 'put'
            // needs to force a table-resize for a too-long key-reprobe sequence.
            // Check for too-many-reprobes on get - and flip to the new table.
            if (++reprobe_cnt >= reprobe_limit(len) || // too many probes
                    key == TOMBSTONE) // found a TOMBSTONE key, means no more keys in this table
                return newkvs == null ? null : get_impl(topmap, topmap.help_copy(newkvs), key, fullhash); // Retry in the new table

            idx = (idx + 1) & (len - 1);    // Reprobe by 1!  (could now prefetch)
        }
    }

    // --- putIfMatch ---------------------------------------------------------
    // Put, Remove, PutIfAbsent, etc.  Return the old value.  If the returned
    // value is equal to expVal (or expVal is NO_MATCH_OLD) then the put can be
    // assumed to work (although might have been immediately overwritten).  Only
    // the path through copy_slot passes in an expected value of null, and
    // putIfMatch only returns a null if passed in an expected null.

    private static final Object putIfMatch(final NonBlockingIdentityHashMap topmap, final Object[] kvs, final Object key, final Object putval, final Object expVal) {
        assert putval != null;
        assert !(putval instanceof Prime);
        assert !(expVal instanceof Prime);
        final int fullhash = hash(key); // throws NullPointerException if key null
        final int len = len(kvs); // Count of key/value pairs, reads kvs.length
        final CHM chm = chm(kvs); // Reads kvs[0]
        int idx = fullhash & (len - 1);

        // ---
        // Key-Claim stanza: spin till we can claim a Key (or force a resizing).
        int reprobe_cnt = 0;
        Object K = null, V = null;
        Object[] newkvs = null;
        while (true) {             // Spin till we get a Key slot
            V = val(kvs, idx);         // Get old value (before volatile read below!)
            K = key(kvs, idx);         // Get current key
            if (K == null) {         // Slot is free?
                // Found an empty Key slot - which means this Key has never been in
                // this table.  No need to put a Tombstone - the Key is not here!
                if (putval == TOMBSTONE) return putval; // Not-now & never-been in this table
                // Claim the null key-slot
                if (CAS_key(kvs, idx, null, key)) { // Claim slot for Key
                    chm._slots.add(1);      // Raise key-slots-used count
                    break;                  // Got it!
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
                K = key(kvs, idx);       // CAS failed, get updated value
                assert K != null;       // If keys[idx] is null, CAS shoulda worked
            }
            // Key slot was not null, there exists a Key here

            // We need a volatile-read here to preserve happens-before semantics on
            // newly inserted Keys.  If the Key body was written just before inserting
            // into the table a Key-compare here might read the uninitalized Key body.
            // Annoyingly this means we have to volatile-read before EACH key compare.
            newkvs = chm._newkvs;     // VOLATILE READ before key compare

            if (K == key)
                break;                  // Got it!

            // get and put must have the same key lookup logic!  Lest 'get' give
            // up looking too soon.
            //topmap._reprobes.add(1);
            if (++reprobe_cnt >= reprobe_limit(len) || // too many probes or
                    key == TOMBSTONE) { // found a TOMBSTONE key, means no more keys
                // We simply must have a new table to do a 'put'.  At this point a
                // 'get' will also go to the new table (if any).  We do not need
                // to claim a key slot (indeed, we cannot find a free one to claim!).
                newkvs = chm.resize(topmap, kvs);
                if (expVal != null) topmap.help_copy(newkvs); // help along an existing copy
                return putIfMatch(topmap, newkvs, key, putval, expVal);
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
        // not-null (i.e., once per key-insert).  Of course we got a 'free' check
        // of newkvs once per key-compare (not really free, but paid-for by the
        // time we get here).
        if (newkvs == null &&       // New table-copy already spotted?
                // Once per fresh key-insert check the hard way
                ((V == null && chm.tableFull(reprobe_cnt, len)) ||
                        // Or we found a Prime, but the JMM allowed reordering such that we
                        // did not spot the new table (very rare race here: the writing
                        // thread did a CAS of _newkvs then a store of a Prime.  This thread
                        // reads the Prime, then reads _newkvs - but the read of Prime was so
                        // delayed (or the read of _newkvs was so accelerated) that they
                        // swapped and we still read a null _newkvs.  The resize call below
                        // will do a CAS on _newkvs forcing the read.
                        V instanceof Prime))
            newkvs = chm.resize(topmap, kvs); // Force the new table copy to start
        // See if we are moving to a new table.
        // If so, copy our slot and retry in the new table.
        if (newkvs != null)
            return putIfMatch(topmap, chm.copy_slot_and_check(topmap, kvs, idx, expVal), key, putval, expVal);

        // ---
        // We are finally prepared to update the existing table
        while (true) {
            assert !(V instanceof Prime);

            // Must match old, and we do not?  Then bail out now.  Note that either V
            // or expVal might be TOMBSTONE.  Also V can be null, if we've never
            // inserted a value before.  expVal can be null if we are called from
            // copy_slot.

            if (expVal != NO_MATCH_OLD && // Do we care about expected-Value at all?
                    V != expVal &&            // No instant match already?
                    (expVal != MATCH_ANY || V == TOMBSTONE || V == null) &&
                    !(V == null && expVal == TOMBSTONE) &&    // Match on null/TOMBSTONE combo
                    (expVal == null || !expVal.equals(V))) // Expensive equals check at the last
                return V;                                 // Do not update!

            // Actually change the Value in the Key,Value pair
            if (CAS_val(kvs, idx, V, putval)) {
                // CAS succeeded - we did the update!
                // Both normal put's and table-copy calls putIfMatch, but table-copy
                // does not (effectively) increase the number of live k/v pairs.
                if (expVal != null) {
                    // Adjust sizes - a striped counter
                    if ((V == null || V == TOMBSTONE) && putval != TOMBSTONE) chm._size.add(1);
                    if (!(V == null || V == TOMBSTONE) && putval == TOMBSTONE) chm._size.add(-1);
                }
                return (V == null && expVal != null) ? TOMBSTONE : V;
            }
            // Else CAS failed
            V = val(kvs, idx);         // Get new value
            // If a Prime'd value got installed, we need to re-run the put on the
            // new table.  Otherwise we lost the CAS to another racing put.
            // Simply retry from the start.
            if (V instanceof Prime)
                return putIfMatch(topmap, chm.copy_slot_and_check(topmap, kvs, idx, expVal), key, putval, expVal);
        }
    }

    // --- help_copy ---------------------------------------------------------
    // Help along an existing resize operation.  This is just a fast cut-out
    // wrapper, to encourage inlining for the fast no-copy-in-progress case.  We
    // always help the top-most table copy, even if there are nested table
    // copies in progress.

    private final Object[] help_copy(Object[] helper) {
        // Read the top-level KVS only once.  We'll try to help this copy along,
        // even if it gets promoted out from under us (i.e., the copy completes
        // and another KVS becomes the top-level copy).
        Object[] topkvs = _kvs;
        CHM topchm = chm(topkvs);
        if (topchm._newkvs == null) return helper; // No copy in-progress
        topchm.help_copy_impl(this, topkvs, false);
        return helper;
    }


    // --- CHM -----------------------------------------------------------------
    // The control structure for the NonBlockingIdentityHashMap

    private static final class CHM<TypeK, TypeV> {
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
        // The 'new KVs' array - created during a resize operation.  This
        // represents the new table being copied from the old one.  It's the
        // volatile variable that is read as we cross from one table to the next,
        // to get the required memory orderings.  It monotonically transits from
        // null to set (once).
        volatile Object[] _newkvs;
        private final AtomicReferenceFieldUpdater<CHM, Object[]> _newkvsUpdater =
                AtomicReferenceFieldUpdater.newUpdater(CHM.class, Object[].class, "_newkvs");

        // Set the _next field if we can.

        boolean CAS_newkvs(Object[] newkvs) {
            while (_newkvs == null)
                if (_newkvsUpdater.compareAndSet(this, null, newkvs))
                    return true;
            return false;
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
        volatile long _resizers; // count of threads attempting an initial resize
        private static final AtomicLongFieldUpdater<CHM> _resizerUpdater =
                AtomicLongFieldUpdater.newUpdater(CHM.class, "_resizers");

        // ---
        // Simple constructor

        CHM(Counter size) {
            _size = size;
            _slots = new Counter();
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

        private final Object[] resize(NonBlockingIdentityHashMap topmap, Object[] kvs) {
            assert chm(kvs) == this;

            // Check for resize already in progress, probably triggered by another thread
            Object[] newkvs = _newkvs; // VOLATILE READ
            if (newkvs != null)       // See if resize is already in progress
                return newkvs;           // Use the new table already

            // No copy in-progress, so start one.  First up: compute new table size.
            int oldlen = len(kvs);    // Old count of K,V pairs allowed
            int sz = size();          // Get current table count of active K,V pairs
            int newsz = sz;           // First size estimate

            // Heuristic to determine new size.  We expect plenty of dead-slots-with-keys
            // and we need some decent padding to avoid endless reprobing.
            if (sz >= (oldlen >> 2)) { // If we are >25% full of keys then...
                newsz = oldlen << 1;      // Double size
                if (sz >= (oldlen >> 1)) // If we are >50% full of keys then...
                    newsz = oldlen << 2;    // Double double size
            }
            // This heuristic in the next 2 lines leads to a much denser table
            // with a higher reprobe rate
            //if( sz >= (oldlen>>1) ) // If we are >50% full of keys then...
            //  newsz = oldlen<<1;    // Double size

            // Last (re)size operation was very recent?  Then double again; slows
            // down resize operations for tables subject to a high key churn rate.
            long tm = System.currentTimeMillis();
            long q = 0;
            if (newsz <= oldlen && // New table would shrink or hold steady?
                    tm <= topmap._last_resize_milli + 10000 && // Recent resize (less than 1 sec ago)
                    (q = _slots.estimate_get()) >= (sz << 1)) // 1/2 of keys are dead?
                newsz = oldlen << 1;      // Double the existing size

            // Do not shrink, ever
            if (newsz < oldlen) newsz = oldlen;

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
                newkvs = _newkvs;        // Between dorking around, another thread did it
                if (newkvs != null)     // See if resize is already in progress
                    return newkvs;         // Use the new table already
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
            newkvs = _newkvs;
            if (newkvs != null)      // See if resize is already in progress
                return newkvs;          // Use the new table already

            // Double size for K,V pairs, add 1 for CHM
            newkvs = new Object[((1 << log2) << 1) + 2]; // This can get expensive for big arrays
            newkvs[0] = new CHM(_size); // CHM in slot 0
            newkvs[1] = new int[1 << log2]; // hashes in slot 1

            // Another check after the slow allocation
            if (_newkvs != null)     // See if resize is already in progress
                return _newkvs;         // Use the new table already

            // The new table must be CAS'd in so only 1 winner amongst duplicate
            // racing resizing threads.  Extra CHM's will be GC'd.
            if (CAS_newkvs(newkvs)) { // NOW a resize-is-in-progress!
                //notifyAll();            // Wake up any sleepers
                //long nano = System.nanoTime();
                //System.out.println(" "+nano+" Resize from "+oldlen+" to "+(1<<log2)+" and had "+(_resizers-1)+" extras" );
                //if( System.out != null ) System.out.print("["+log2);
                topmap.rehash();        // Call for Hashtable's benefit
            } else                    // CAS failed?
                newkvs = _newkvs;       // Reread new table
            return newkvs;
        }


        // The next part of the table to copy.  It monotonically transits from zero
        // to _kvs.length.  Visitors to the table can claim 'work chunks' by
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

        private final void help_copy_impl(NonBlockingIdentityHashMap topmap, Object[] oldkvs, boolean copy_all) {
            assert chm(oldkvs) == this;
            Object[] newkvs = _newkvs;
            assert newkvs != null;    // Already checked by caller
            int oldlen = len(oldkvs); // Total amount to copy
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
                        copyidx = (int) _copyIdx;      // Re-read
                    if (!(copyidx < (oldlen << 1)))  // Panic!
                        panic_start = copyidx;        // Record where we started to panic-copy
                }

                // We now know what to copy.  Try to copy.
                int workdone = 0;
                for (int i = 0; i < MIN_COPY_WORK; i++)
                    if (copy_slot(topmap, (copyidx + i) & (oldlen - 1), oldkvs, newkvs)) // Made an oldtable slot go dead?
                        workdone++;         // Yes!
                if (workdone > 0)      // Report work-done occasionally
                    copy_check_and_promote(topmap, oldkvs, workdone);// See if we can promote
                //for( int i=0; i<MIN_COPY_WORK; i++ )
                //  if( copy_slot(topmap,(copyidx+i)&(oldlen-1),oldkvs,newkvs) ) // Made an oldtable slot go dead?
                //    copy_check_and_promote( topmap, oldkvs, 1 );// See if we can promote

                copyidx += MIN_COPY_WORK;
                // Uncomment these next 2 lines to turn on incremental table-copy.
                // Otherwise this thread continues to copy until it is all done.
                if (!copy_all && panic_start == -1) // No panic?
                    return;       // Then done copying after doing MIN_COPY_WORK
            }
            // Extra promotion check, in case another thread finished all copying
            // then got stalled before promoting.
            copy_check_and_promote(topmap, oldkvs, 0);// See if we can promote
        }


        // --- copy_slot_and_check -----------------------------------------------
        // Copy slot 'idx' from the old table to the new table.  If this thread
        // confirmed the copy, update the counters and check for promotion.
        //
        // Returns the result of reading the volatile _newkvs, mostly as a
        // convenience to callers.  We come here with 1-shot copy requests
        // typically because the caller has found a Prime, and has not yet read
        // the _newkvs volatile - which must have changed from null-to-not-null
        // before any Prime appears.  So the caller needs to read the _newkvs
        // field to retry his operation in the new table, but probably has not
        // read it yet.

        private final Object[] copy_slot_and_check(NonBlockingIdentityHashMap topmap, Object[] oldkvs, int idx, Object should_help) {
            assert chm(oldkvs) == this;
            Object[] newkvs = _newkvs; // VOLATILE READ
            // We're only here because the caller saw a Prime, which implies a
            // table-copy is in progress.
            assert newkvs != null;
            if (copy_slot(topmap, idx, oldkvs, _newkvs))   // Copy the desired slot
                copy_check_and_promote(topmap, oldkvs, 1); // Record the slot copied
            // Generically help along any copy (except if called recursively from a helper)
            return (should_help == null) ? newkvs : topmap.help_copy(newkvs);
        }

        // --- copy_check_and_promote --------------------------------------------

        private final void copy_check_and_promote(NonBlockingIdentityHashMap topmap, Object[] oldkvs, int workdone) {
            assert chm(oldkvs) == this;
            int oldlen = len(oldkvs);
            // We made a slot unusable and so did some of the needed copy work
            long copyDone = _copyDone;
            assert (copyDone + workdone) <= oldlen;
            if (workdone > 0) {
                while (!_copyDoneUpdater.compareAndSet(this, copyDone, copyDone + workdone)) {
                    copyDone = _copyDone; // Reload, retry
                    assert (copyDone + workdone) <= oldlen;
                }
                //if( (10*copyDone/oldlen) != (10*(copyDone+workdone)/oldlen) )
                //System.out.print(" "+(copyDone+workdone)*100/oldlen+"%"+"_"+(_copyIdx*100/oldlen)+"%");
            }

            // Check for copy being ALL done, and promote.  Note that we might have
            // nested in-progress copies and manage to finish a nested copy before
            // finishing the top-level copy.  We only promote top-level copies.
            if (copyDone + workdone == oldlen && // Ready to promote this table?
                    topmap._kvs == oldkvs && // Looking at the top-level table?
                    // Attempt to promote
                    topmap.CAS_kvs(oldkvs, _newkvs)) {
                topmap._last_resize_milli = System.currentTimeMillis(); // Record resize time for next check
                //long nano = System.nanoTime();
                //System.out.println(" "+nano+" Promote table to "+len(_newkvs));
                //if( System.out != null ) System.out.print("]");
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

        private boolean copy_slot(NonBlockingIdentityHashMap topmap, int idx, Object[] oldkvs, Object[] newkvs) {
            // Blindly set the key slot from null to TOMBSTONE, to eagerly stop
            // fresh put's from inserting new values in the old table when the old
            // table is mid-resize.  We don't need to act on the results here,
            // because our correctness stems from box'ing the Value field.  Slamming
            // the Key field is a minor speed optimization.
            Object key;
            while ((key = key(oldkvs, idx)) == null)
                CAS_key(oldkvs, idx, null, TOMBSTONE);

            // ---
            // Prevent new values from appearing in the old table.
            // Box what we see in the old table, to prevent further updates.
            Object oldval = val(oldkvs, idx); // Read OLD table
            while (!(oldval instanceof Prime)) {
                final Prime box = (oldval == null || oldval == TOMBSTONE) ? TOMBPRIME : new Prime(oldval);
                if (CAS_val(oldkvs, idx, oldval, box)) { // CAS down a box'd version of oldval
                    // If we made the Value slot hold a TOMBPRIME, then we both
                    // prevented further updates here but also the (absent)
                    // oldval is vaccuously available in the new table.  We
                    // return with true here: any thread looking for a value for
                    // this key can correctly go straight to the new table and
                    // skip looking in the old table.
                    if (box == TOMBPRIME)
                        return true;
                    // Otherwise we boxed something, but it still needs to be
                    // copied into the new table.
                    oldval = box;         // Record updated oldval
                    break;                // Break loop; oldval is now boxed by us
                }
                oldval = val(oldkvs, idx); // Else try, try again
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
            boolean copied_into_new = (putIfMatch(topmap, newkvs, key, old_unboxed, null) == null);

            // ---
            // Finally, now that any old value is exposed in the new table, we can
            // forever hide the old-table value by slapping a TOMBPRIME down.  This
            // will stop other threads from uselessly attempting to copy this slot
            // (i.e., it's a speed optimization not a correctness issue).
            while (!CAS_val(oldkvs, idx, oldval, TOMBPRIME))
                oldval = val(oldkvs, idx);

            return copied_into_new;
        } // end copy_slot
    } // End of CHM


    // --- Snapshot ------------------------------------------------------------
    // The main class for iterating over the NBHM.  It "snapshots" a clean
    // view of the K/V array.

    private class SnapshotV implements Iterator<TypeV>, Enumeration<TypeV> {
        final Object[] _sskvs;

        public SnapshotV() {
            while (true) {           // Verify no table-copy-in-progress
                Object[] topkvs = _kvs;
                CHM topchm = chm(topkvs);
                if (topchm._newkvs == null) { // No table-copy-in-progress
                    // The "linearization point" for the iteration.  Every key in this
                    // table will be visited, but keys added later might be skipped or
                    // even be added to a following table (also not iterated over).
                    _sskvs = topkvs;
                    break;
                }
                // Table copy in-progress - so we cannot get a clean iteration.  We
                // must help finish the table copy before we can start iterating.
                topchm.help_copy_impl(NonBlockingIdentityHashMap.this, topkvs, true);
            }
            // Warm-up the iterator
            next();
        }

        int length() {
            return len(_sskvs);
        }

        Object key(int idx) {
            return NonBlockingIdentityHashMap.key(_sskvs, idx);
        }

        private int _idx;              // Varies from 0-keys.length
        private Object _nextK, _prevK; // Last 2 keys found
        private TypeV _nextV, _prevV; // Last 2 values found

        public boolean hasNext() {
            return _nextV != null;
        }

        public TypeV next() {
            // 'next' actually knows what the next value will be - it had to
            // figure that out last go-around lest 'hasNext' report true and
            // some other thread deleted the last value.  Instead, 'next'
            // spends all its effort finding the key that comes after the
            // 'next' key.
            if (_idx != 0 && _nextV == null) throw new NoSuchElementException();
            _prevK = _nextK;          // This will become the previous key
            _prevV = _nextV;          // This will become the previous value
            _nextV = null;            // We have no more next-key
            // Attempt to set <_nextK,_nextV> to the next K,V pair.
            // _nextV is the trigger: stop searching when it is != null
            while (_idx < length()) {  // Scan array
                _nextK = key(_idx++); // Get a key that definitely is in the set (for the moment!)
                if (_nextK != null && // Found something?
                        _nextK != TOMBSTONE &&
                        (_nextV = get(_nextK)) != null)
                    break;                // Got it!  _nextK is a valid Key
            }                         // Else keep scanning
            return _prevV;            // Return current value.
        }

        public void remove() {
            if (_prevV == null) throw new IllegalStateException();
            putIfMatch(NonBlockingIdentityHashMap.this, _sskvs, _prevK, TOMBSTONE, _prevV);
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
    @Override
    public Collection<TypeV> values() {
        return new AbstractCollection<TypeV>() {
            @Override public void clear() {
                NonBlockingIdentityHashMap.this.clear();
            }

            @Override public int size() {
                return NonBlockingIdentityHashMap.this.size();
            }

            @Override public boolean contains(Object v) {
                return NonBlockingIdentityHashMap.this.containsValue(v);
            }

            @Override public Iterator<TypeV> iterator() {
                return new SnapshotV();
            }
        };
    }

    // --- keySet --------------------------------------------------------------

    private class SnapshotK implements Iterator<TypeK>, Enumeration<TypeK> {
        final SnapshotV _ss;

        public SnapshotK() {
            _ss = new SnapshotV();
        }

        public void remove() {
            _ss.remove();
        }

        public TypeK next() {
            _ss.next();
            return (TypeK) _ss._prevK;
        }

        public boolean hasNext() {
            return _ss.hasNext();
        }

        public TypeK nextElement() {
            return next();
        }

        public boolean hasMoreElements() {
            return hasNext();
        }
    }

    /**
     * Returns an enumeration of the keys in this table.
     *
     * @return an enumeration of the keys in this table
     * @see #keySet()
     */
    public Enumeration<TypeK> keys() {
        return new SnapshotK();
    }

    /**
     * Returns a {@link Set} view of the keys contained in this map.  The set
     * is backed by the map, so changes to the map are reflected in the set,
     * and vice-versa.  The set supports element removal, which removes the
     * corresponding mapping from this map, via the <tt>Iterator.remove</tt>,
     * <tt>Set.remove</tt>, <tt>removeAll</tt>, <tt>retainAll</tt>, and
     * <tt>clear</tt> operations.  It does not support the <tt>add</tt> or
     * <tt>addAll</tt> operations.
     * <p/>
     * <p>The view's <tt>iterator</tt> is a "weakly consistent" iterator that
     * will never throw {@link ConcurrentModificationException}, and guarantees
     * to traverse elements as they existed upon construction of the iterator,
     * and may (but is not guaranteed to) reflect any modifications subsequent
     * to construction.
     */
    @Override
    public Set<TypeK> keySet() {
        return new AbstractSet<TypeK>() {
            @Override public void clear() {
                NonBlockingIdentityHashMap.this.clear();
            }

            @Override public int size() {
                return NonBlockingIdentityHashMap.this.size();
            }

            @Override public boolean contains(Object k) {
                return NonBlockingIdentityHashMap.this.containsKey(k);
            }

            @Override public boolean remove(Object k) {
                return NonBlockingIdentityHashMap.this.remove(k) != null;
            }

            @Override public Iterator<TypeK> iterator() {
                return new SnapshotK();
            }
        };
    }


    // --- entrySet ------------------------------------------------------------
    // Warning: Each call to 'next' in this iterator constructs a new NBHMEntry.

    private class NBHMEntry extends AbstractEntry<TypeK, TypeV> {
        NBHMEntry(final TypeK k, final TypeV v) {
            super(k, v);
        }

        public TypeV setValue(final TypeV val) {
            if (val == null) throw new NullPointerException();
            _val = val;
            return put(_key, val);
        }
    }

    private class SnapshotE implements Iterator<Map.Entry<TypeK, TypeV>> {
        final SnapshotV _ss;

        public SnapshotE() {
            _ss = new SnapshotV();
        }

        public void remove() {
            _ss.remove();
        }

        public Map.Entry<TypeK, TypeV> next() {
            _ss.next();
            return new NBHMEntry((TypeK) _ss._prevK, _ss._prevV);
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
     * iteration.  The {@link NonBlockingIdentityHashMap} does not normally create or
     * using {@link java.util.Map.Entry} objects so they will be created soley
     * to support this iteration.  Iterating using {@link #keySet} or {@link
     * #values} will be more efficient.
     */
    @Override
    public Set<Map.Entry<TypeK, TypeV>> entrySet() {
        return new AbstractSet<Map.Entry<TypeK, TypeV>>() {
            @Override public void clear() {
                NonBlockingIdentityHashMap.this.clear();
            }

            @Override public int size() {
                return NonBlockingIdentityHashMap.this.size();
            }

            @Override public boolean remove(final Object o) {
                if (!(o instanceof Map.Entry)) return false;
                final Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
                return NonBlockingIdentityHashMap.this.remove(e.getKey(), e.getValue());
            }

            @Override public boolean contains(final Object o) {
                if (!(o instanceof Map.Entry)) return false;
                final Map.Entry<?, ?> e = (Map.Entry<?, ?>) o;
                TypeV v = get(e.getKey());
                return v.equals(e.getValue());
            }

            @Override public Iterator<Map.Entry<TypeK, TypeV>> iterator() {
                return new SnapshotE();
            }
        };
    }

    // --- writeObject -------------------------------------------------------
    // Write a NBHM to a stream

    private void writeObject(java.io.ObjectOutputStream s) throws IOException {
        s.defaultWriteObject();     // Nothing to write
        for (Object K : keySet()) {
            final Object V = get(K);  // Do an official 'get'
            s.writeObject(K);         // Write the <TypeK,TypeV> pair
            s.writeObject(V);
        }
        s.writeObject(null);        // Sentinel to indicate end-of-data
        s.writeObject(null);
    }

    // --- readObject --------------------------------------------------------
    // Read a CHM from a stream

    private void readObject(java.io.ObjectInputStream s) throws IOException, ClassNotFoundException {
        s.defaultReadObject();      // Read nothing
        initialize(MIN_SIZE);
        for (; ;) {
            final TypeK K = (TypeK) s.readObject();
            final TypeV V = (TypeV) s.readObject();
            if (K == null) break;
            put(K, V);                 // Insert with an offical put
        }
    }

} // End NonBlockingIdentityHashMap class
