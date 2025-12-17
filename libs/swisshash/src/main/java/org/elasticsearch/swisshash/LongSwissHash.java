/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.swisshash;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorSpecies;

import com.carrotsearch.hppc.BitMixer;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.LongHashTable;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Assigns {@code int} ids to {@code long}s, vending the ids in order they are added.
 *
 * <p> At it's core there are two hash table implementations, a "small core" and
 * a "big core". The "small core" is a simple
 * <a href="https://en.wikipedia.org/wiki/Open_addressing">open addressed</a>
 * hash table with a fixed 60% load factor and a table of 2048. It's quite quick
 * because it has a fixed size and never grows.
 *
 * <p> When the "small core" has more entries than it's load factor the "small core"
 * is replaced with a "big core". The "big core" functions quite similarly to
 * a <a href="https://faultlore.com/blah/hashbrown-tldr/">Swisstable</a>, Google's
 * fancy SIMD hash table. In this table there's a contiguous array of "control"
 * bytes that are either {@code 0b1111_1111} for empty entries or
 * {@code 0b0aaa_aaaa} for populated entries, where {@code aaa_aaaa} are the top
 * 7 bytes of the hash. To find an entry by key you hash it, grab the top 7 bytes
 * or it, and perform a SIMD read of the control array starting at the expected
 * slot. We use the widest SIMD instruction the CPU supports, meaning 64 or 32
 * bytes. If any of those match we check the actual key. So instead of scanning
 * one slot at a time "small core", we effectively scan a whole bunch at once.
 * This allows us to run a much higher load factor (87.5%) without any performance
 * penalty so the extra byte feels super worth it.
 *
 * <p> When a "big core" fills it's table to the fill factor, we build a new
 * "big core" and read all values in the old "big core" into the new one.
 *
 * <p> This class does not store the keys in the hash table slots. Instead, it
 * uses a {@link #keyPages} to store the actual values, and the hash table
 * slots store the {@code id} which indexes into the {@link #keyPages}.
 */
public final class LongSwissHash extends SwissHash implements LongHashTable {

    static final VectorSpecies<Byte> BS = ByteVector.SPECIES_128;

    private static final int BYTE_VECTOR_LANES = BS.vectorByteSize();

    private static final int PAGE_SHIFT = 14;

    private static final int PAGE_MASK = PageCacheRecycler.PAGE_SIZE_IN_BYTES - 1;

    private static final int KEY_SIZE = Long.BYTES;

    private static final int ID_SIZE = Integer.BYTES;

    static final int INITIAL_CAPACITY = PageCacheRecycler.PAGE_SIZE_IN_BYTES / KEY_SIZE;

    static {
        if (PageCacheRecycler.PAGE_SIZE_IN_BYTES >> PAGE_SHIFT != 1) {
            throw new AssertionError("bad constants");
        }
        if (Integer.highestOneBit(KEY_SIZE) != KEY_SIZE) {
            throw new AssertionError("not a power of two");
        }
        if (Integer.highestOneBit(ID_SIZE) != ID_SIZE) {
            throw new AssertionError("not a power of two");
        }
        if (Integer.highestOneBit(INITIAL_CAPACITY) != INITIAL_CAPACITY) {
            throw new AssertionError("not a power of two");
        }
        if (ID_SIZE > KEY_SIZE) {
            throw new AssertionError("key too small");
        }
    }

    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());

    /**
     * Pages of {@code keys}, vended by the {@link PageCacheRecycler}. It's
     * important that the size of keys be a power of two, so we can quickly
     * select the appropriate page and keys never span multiple pages.
     */
    private byte[][] keyPages;
    private SmallCore smallCore;
    private BigCore bigCore;
    private final List<Releasable> toClose = new ArrayList<>();

    LongSwissHash(PageCacheRecycler recycler, CircuitBreaker breaker) {
        super(recycler, breaker, INITIAL_CAPACITY, LongSwissHash.SmallCore.FILL_FACTOR);
        boolean success = false;
        try {
            smallCore = new SmallCore();
            keyPages = new byte[][] { grabKeyPage() };
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    private byte[] grabKeyPage() {
        breaker.addEstimateBytesAndMaybeBreak(PageCacheRecycler.PAGE_SIZE_IN_BYTES, "LongSwissHash-keyPage");
        toClose.add(() -> breaker.addWithoutBreaking(-PageCacheRecycler.PAGE_SIZE_IN_BYTES));
        Recycler.V<byte[]> page = recycler.bytePage(false);
        toClose.add(page);
        return page.v();
    }

    /**
     * Finds an {@code id} by a {@code key}.
     */
    @Override
    public long find(final long key) {
        final int hash = hash(key);
        if (smallCore != null) {
            return smallCore.find(key, hash);
        } else {
            return bigCore.find(key, hash, control(hash));
        }
    }

    /**
     * Adds many {@code key}s at once, putting their {@code id}s into an array.
     * If any {@code key} was already present it's previous assigned {@code id}
     * will be added to the array. If it wasn't present it'll be assigned a new
     * {@code id}.
     *
     * <p> This method tends to be faster than {@link #add(long)}.
     */
    public void add(long[] keys, long[] ids, int length) {
        int i = 0;
        for (; i < length; i++) {
            if (bigCore != null) {
                for (; i < length; i++) {
                    long k = keys[i];
                    ids[i] = bigCore.add(k, hash(k));
                }
                return;
            }

            ids[i] = Math.toIntExact(add(keys[i]));
        }
    }

    /**
     * Add a {@code key}, returning its {@code id}s. If it was already present
     * it's previous assigned {@code id} will be returned. If it wasn't present
     * it'll be assigned a new {@code id}.
     */
    @Override
    public long add(final long key) {
        final int hash = hash(key);
        if (smallCore != null) {
            if (size < nextGrowSize) {
                return smallCore.add(key, hash);
            }
            smallCore.transitionToBigCore();
        }
        return bigCore.add(key, hash);
    }

    @Override
    public Status status() {
        return smallCore != null ? smallCore.status() : bigCore.status();
    }

    public abstract class Itr extends SwissHash.Itr {
        /**
         * The key the iterator current points to.
         */
        public abstract long key();
    }

    @Override
    public Itr iterator() {
        return smallCore != null ? smallCore.iterator() : bigCore.iterator();
    }

    /**
     * Build the control byte for a populated entry out of the hash.
     * The control bytes for a populated entry has the high bit clear
     * and the remaining 7 bits contain the top 7 bits of the hash.
     * So it looks like {@code 0b0xxx_xxxx}.
     */
    private static byte control(int hash) {
        return (byte) (hash >>> (Integer.SIZE - 7));
    }

    @Override
    public void close() {
        Releasables.close(smallCore, bigCore);
        Releasables.close(toClose);
        toClose.clear();
    }

    private int growTracking() {
        // Juggle constants for the new page size
        growCount++;
        int oldCapacity = capacity;
        capacity <<= 1;
        if (capacity < 0) {
            throw new IllegalArgumentException("overflow: oldCapacity=" + oldCapacity + ", new capacity=" + capacity);
        }
        mask = capacity - 1;
        nextGrowSize = (int) (capacity * BigCore.FILL_FACTOR);
        return oldCapacity;
    }

    /**
     * Open addressed hash table twith linear probing. Empty {@code id}s are
     * encoded as {@code -1}. This hash table can't grow, and is instead
     * replaced by a {@link BigCore}.
     *
     * <p> This uses one page from the {@link PageCacheRecycler} for the
     * {@code ids}.
     */
    final class SmallCore extends Core {
        static final float FILL_FACTOR = 0.6F;

        private final byte[] idPage;

        private SmallCore() {
            boolean success = false;
            try {
                idPage = grabPage();
                Arrays.fill(idPage, (byte) 0xff);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        int find(final long key, final int hash) {
            int slot = slot(hash);
            for (;;) {
                int id = id(slot);
                if (id < 0) {
                    return -1; // empty
                }
                if (key(id) == key) {
                    return id;
                }
                slot = slot(slot + 1);
            }
        }

        int add(final long key, final int hash) {
            int slot = slot(hash);
            for (;;) {
                final int idOffset = idOffset(slot);
                final int currentId = (int) INT_HANDLE.get(idPage, idOffset);
                if (currentId >= 0) {
                    final long currentKey = key(currentId);
                    if (currentKey == key) {
                        return -1 - currentId;
                    }
                    slot = slot(slot + 1);
                } else {
                    int id = size;
                    INT_HANDLE.set(idPage, idOffset, id);
                    setKey(id, key);
                    size++;
                    return id;
                }
            }
        }

        void transitionToBigCore() {
            growTracking();
            try {
                bigCore = new BigCore();
                rehash();
            } finally {
                close();
                smallCore = null;
            }
        }

        @Override
        protected Status status() {
            return new SmallCoreStatus(growCount, capacity, size, nextGrowSize);
        }

        @Override
        protected Itr iterator() {
            return new Itr() {
                @Override
                public boolean next() {
                    return ++keyId < size;
                }

                @Override
                public int id() {
                    return keyId;
                }

                @Override
                public long key() {
                    return SmallCore.this.key(keyId);
                }
            };
        }

        private void rehash() {
            for (int i = 0; i < size; i++) {
                final int hash = hash(key(i));
                bigCore.insert(hash, control(hash), i);
            }
        }

        private long key(int id) {
            return (long) LONG_HANDLE.get(keyPages[0], keyOffset(id));
        }

        private void setKey(int id, long value) {
            LONG_HANDLE.set(keyPages[0], keyOffset(id), value);
        }

        private int id(int slot) {
            return (int) INT_HANDLE.get(idPage, idOffset(slot));
        }
    }

    /**
     * A SwissHash inspired hashtable. This differs from the normal SwissHash
     * in because it's adapted to Elasticsearch's {@link PageCacheRecycler}.
     * The keys and ids are stored many {@link PageCacheRecycler#PAGE_SIZE_IN_BYTES}
     * arrays, with the keys separated from the values. This is mostly so that we
     * can be sure the array and offset into the array can be calculated by right
     * shifts.
     */
    final class BigCore extends Core {
        static final float FILL_FACTOR = 0.875F;

        private static final byte EMPTY = (byte) 0x80; // empty slot

        /**
         * The "control" bytes from the SwissHash algorithm. This will contain
         * {@link #EMPTY} for empty entries and {@code 0b0aaa_aaaa} for
         * filled entries, where {@code aaa_aaaa} are the top seven bits of the
         * hash. These are tests by SIMD instructions as a quick first pass to
         * check many entries at once.
         *
         * <p> This array has to be contiguous otherwise we lose too much speed
         * so it isn't managed by the {@link PageCacheRecycler}, instead we
         * allocate it directly.
         *
         * <p> This array contains {@code capacity + SIMD_LANES} entries with the
         * first {@code SIMD_LANES} bytes cloned to the end of the array so the
         * simd probes for possible matches never had to worry about "wrapping"
         * around the array.
         */
        private final byte[] controlData;

        /**
         * Pages of {@code ids}, vended by the {@link PageCacheRecycler}. Ids
         * are {@code int}s so it's very quick to select the appropriate page
         * for each slot.
         */
        private final byte[][] idPages;

        /**
         * The number of times and {@link #add} operation needed to probe additional
         * entries. If all is right with the world this should be {@code 0}, meaning
         * every entry found an empty slot within {@code SIMD_WIDTH} slots from its
         * natural positions. Such hashes will never have to probe on read. More
         * generally, a {@code find} operation should take on average
         * {@code insertProbes / size} probes.
         */
        private int insertProbes;

        BigCore() {
            int controlLength = capacity + BYTE_VECTOR_LANES;
            breaker.addEstimateBytesAndMaybeBreak(controlLength, "LongSwissHash-bigCore");
            toClose.add(() -> breaker.addWithoutBreaking(-controlLength));
            controlData = new byte[controlLength];
            Arrays.fill(controlData, EMPTY);

            boolean success = false;
            try {
                int keyPagesNeeded = (capacity * KEY_SIZE - 1) >> PAGE_SHIFT;
                keyPagesNeeded++;
                var initialKeyPages = keyPages;
                keyPages = new byte[keyPagesNeeded][];
                for (int i = 0; i < keyPagesNeeded; i++) {
                    keyPages[i] = (i < initialKeyPages.length) ? initialKeyPages[i] : grabKeyPage();
                }
                assert keyPages[keyOffset(mask) >> PAGE_SHIFT] != null
                    && Arrays.stream(keyPages).mapToInt(b -> b.length).distinct().count() == 1L
                    && keyPagesNeeded > initialKeyPages.length;

                int idPagesNeeded = (capacity * ID_SIZE - 1) >> PAGE_SHIFT;
                idPagesNeeded++;
                idPages = new byte[idPagesNeeded][];
                for (int i = 0; i < idPagesNeeded; i++) {
                    idPages[i] = grabPage();
                }
                assert idPages[idOffset(mask) >> PAGE_SHIFT] != null;
                success = true;
            } finally {
                if (false == success) {
                    close();
                }
            }
        }

        /**
         * Probes chunks for the value.
         *
         * <p> Each probe is:
         * <ol>
         *   <li>Build a bit mask of all matching control values.</li>
         *   <li>If any match, check if the actual values. If any of those match,
         *       return them.</li>
         *   <li>No values matched, so check the control values for EMPTY flags.
         *       If there are any empty flags, then the value isn't in the hash.</li>
         *   <li>There aren't any EMPTY flags, meaning this chunk is full. So we should
         *       continue probing.</li>
         * </ol>
         *
         * <p> We probe with linear probing.  The probe loop doesn't stop if it
         * never finds an EMPTY flag. But it'll always find one because we keep
         * a load factor lower than 100%.
         */
        private int find(final long key, final int hash, final byte control) {
            int group = hash & mask;
            for (;;) {
                ByteVector vec = ByteVector.fromArray(BS, controlData, group);
                long matches = vec.eq(control).toLong();
                while (matches != 0) {
                    final int checkSlot = slot(group + Long.numberOfTrailingZeros(matches));
                    final int id = id(checkSlot);
                    if (key(id) == key) {
                        return id;
                    }
                    matches &= matches - 1; // clear the first set bit and try again
                }
                long empty = vec.eq(EMPTY).toLong();
                if (empty != 0) {
                    return -1;
                }
                group = slot(group + BYTE_VECTOR_LANES);
            }
        }

        private int add(final long key, final int hash) {
            maybeGrow();
            return bigCore.addImpl(key, hash);
        }

        private int addImpl(final long key, final int hash) {
            final byte control = control(hash);
            int group = hash & mask;
            for (;;) {
                ByteVector vec = ByteVector.fromArray(BS, controlData, group);
                long matches = vec.eq(control).toLong();
                while (matches != 0) {
                    final int checkSlot = slot(group + Long.numberOfTrailingZeros(matches));
                    final int id = id(checkSlot);
                    if (key(id) == key) {
                        return -1 - id;
                    }
                    matches &= matches - 1; // clear the first set bit and try again
                }
                long empty = vec.eq(EMPTY).toLong();
                if (empty != 0) {
                    final int insertSlot = slot(group + Long.numberOfTrailingZeros(empty));
                    final int id = size;
                    setKey(id, key);
                    bigCore.insertAtSlot(insertSlot, control, id);
                    size++;
                    return id;
                }
                group = (group + BYTE_VECTOR_LANES) & mask;
            }
        }

        private void insertAtSlot(final int insertSlot, final byte control, final int id) {
            final int idOffset = idOffset(insertSlot);
            INT_HANDLE.set(idPages[idOffset >> PAGE_SHIFT], idOffset & PAGE_MASK, id);
            controlData[insertSlot] = control;
            // mirror only if slot is within the first group size, to handle wraparound loads
            if (insertSlot < BYTE_VECTOR_LANES) {
                controlData[insertSlot + capacity] = control;
            }
        }

        @Override
        protected Status status() {
            return new BigCoreStatus(growCount, capacity, size, nextGrowSize, insertProbes, keyPages.length, idPages.length);
        }

        @Override
        protected Itr iterator() {
            return new Itr() {
                @Override
                public boolean next() {
                    return ++keyId < size;
                }

                @Override
                public int id() {
                    return keyId;
                }

                @Override
                public long key() {
                    return BigCore.this.key(keyId);
                }
            };
        }

        private void maybeGrow() {
            if (size >= nextGrowSize) {
                assert size == nextGrowSize;
                grow();
            }
        }

        private void grow() {
            growTracking();
            try {
                var newBigCore = new BigCore();
                rehash(newBigCore);
                bigCore = newBigCore;
            } finally {
                close();
            }
        }

        private void rehash(BigCore newBigCore) {
            for (int i = 0; i < size; i++) {
                final int hash = hash(key(i));
                newBigCore.insert(hash, control(hash), i);
            }
        }

        /**
         * Inserts the key into the first empty slot that allows it. Used
         * by {@link #rehash} because we know all keys are unique.
         */
        private void insert(final int hash, final byte control, final int id) {
            int group = hash & mask;
            for (;;) {
                for (int j = 0; j < BYTE_VECTOR_LANES; j++) {
                    int idx = group + j;
                    if (controlData[idx] == EMPTY) {
                        int insertSlot = slot(group + j);
                        insertAtSlot(insertSlot, control, id);
                        return;
                    }
                }
                group = (group + BYTE_VECTOR_LANES) & mask;
                insertProbes++;
            }
        }

        private long key(final int id) {
            final int keyOffset = keyOffset(id);
            return (long) LONG_HANDLE.get(keyPages[keyOffset >> PAGE_SHIFT], keyOffset & PAGE_MASK);
        }

        private void setKey(final int id, final long value) {
            final int keyOffset = keyOffset(id);
            LONG_HANDLE.set(keyPages[keyOffset >> PAGE_SHIFT], keyOffset & PAGE_MASK, value);
        }

        private int id(final int slot) {
            final int idOffset = idOffset(slot);
            return (int) INT_HANDLE.get(idPages[idOffset >> PAGE_SHIFT], idOffset & PAGE_MASK);
        }
    }

    /**
     * Returns the key at <code>0 &lt;= id &lt;= size()</code>.
     * The result is undefined if the id is unused.
     * @param id the id returned when the key was added
     * @return the key
     */
    public long get(final long id) {
        final int actualId = Math.toIntExact(id);
        Objects.checkIndex(actualId, size());
        return smallCore != null ? smallCore.key(actualId) : bigCore.key(actualId);
    }

    private int keyOffset(final int id) {
        return id * KEY_SIZE;
    }

    private int idOffset(final int slot) {
        return slot * ID_SIZE;
    }

    private int hash(final long v) {
        return BitMixer.mix(v);
    }

    private int slot(final int hash) {
        return hash & mask;
    }
}
