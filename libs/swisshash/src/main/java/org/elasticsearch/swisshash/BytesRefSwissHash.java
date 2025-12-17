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

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Objects;

/**
 * Assigns {@code int} ids to {@code BytesRef}s, vending the ids in order they are added.
 *
 * <p> At it's core there are two hash table implementations, a "small core" and
 * a "big core". The "small core" is a simple
 * <a href="https://en.wikipedia.org/wiki/Open_addressing">open addressed</a>
 * hash table with a fixed 60% load factor and a table of 2048. It quite quick
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
 * "big core" nd read all values in the old "big core" into the new one.
 *
 * <p> This class does not store the keys in the hash table slots. Instead, it
 * uses a {@link BytesRefArray} to store the actual bytes, and the hash table
 * slots store the {@code id} which indexes into the {@link BytesRefArray}.
 */
public final class BytesRefSwissHash extends SwissHash implements Accountable, BytesRefHashTable {

    // base size of the bytes ref hash
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BytesRefSwissHash.class)
        // spare BytesRef
        + RamUsageEstimator.shallowSizeOfInstance(BytesRef.class);

    private static final VectorSpecies<Byte> BS = ByteVector.SPECIES_128;

    private static final int BYTE_VECTOR_LANES = BS.vectorByteSize();

    private static final int PAGE_SHIFT = 14;

    private static final int PAGE_MASK = PageCacheRecycler.PAGE_SIZE_IN_BYTES - 1;

    private static final int ID_AND_HASH_SIZE = Long.BYTES;

    // We use a smaller initial capacity than LongSwissHash because we don't store keys in pages,
    // but we want to be consistent with the page-based sizing logic.
    // PAGE_SIZE / ID_AND_HASH_SIZE = 16384 / 8 = 2048.
    static final int INITIAL_CAPACITY = PageCacheRecycler.PAGE_SIZE_IN_BYTES / ID_AND_HASH_SIZE;

    static {
        if (PageCacheRecycler.PAGE_SIZE_IN_BYTES >> PAGE_SHIFT != 1) {
            throw new AssertionError("bad constants");
        }
        if (Integer.highestOneBit(ID_AND_HASH_SIZE) != ID_AND_HASH_SIZE) {
            throw new AssertionError("not a power of two");
        }
        if (Integer.highestOneBit(INITIAL_CAPACITY) != INITIAL_CAPACITY) {
            throw new AssertionError("not a power of two");
        }
    }

    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());

    private final BytesRefArray bytesRefs;
    private final boolean ownsBytesRefs;
    private final BytesRef scratch = new BytesRef();

    private SmallCore smallCore;
    private BigCore bigCore;

    /**
     * Creates a new {@link BytesRefSwissHash} that manages its own {@link BytesRefArray}.
     */
    BytesRefSwissHash(PageCacheRecycler recycler, CircuitBreaker breaker, BigArrays bigArrays) {
        this(recycler, breaker, new BytesRefArray(PageCacheRecycler.PAGE_SIZE_IN_BYTES, bigArrays), true);
    }

    /**
     * Creates a new {@link BytesRefSwissHash} that uses the provided {@link BytesRefArray}.
     * This allows multiple {@link BytesRefSwissHash} to share the same key storage and ID space.
     */
    BytesRefSwissHash(PageCacheRecycler recycler, CircuitBreaker breaker, BytesRefArray bytesRefs) {
        this(recycler, breaker, bytesRefs, false);
    }

    private BytesRefSwissHash(PageCacheRecycler recycler, CircuitBreaker breaker, BytesRefArray bytesRefs, boolean ownsBytesRefs) {
        super(recycler, breaker, INITIAL_CAPACITY, SmallCore.FILL_FACTOR);
        this.bytesRefs = bytesRefs;
        this.ownsBytesRefs = ownsBytesRefs;
        boolean success = false;
        try {
            // If bytesRefs is pre-populated (shared), we don't assume those entries are in this hash.
            // size starts at 0.
            this.size = 0;
            this.smallCore = new SmallCore();
            success = true;
        } finally {
            if (false == success) {
                if (ownsBytesRefs) {
                    Releasables.close(bytesRefs);
                }
            }
        }
    }

    /**
     * Finds an {@code id} by a {@code key}.
     */
    @Override
    public long find(BytesRef key) {
        final int hash = hash(key);
        if (smallCore != null) {
            return smallCore.find(key, hash);
        } else {
            return bigCore.find(key, hash, control(hash));
        }
    }

    /**
     * Adds a {@code key}, returning its {@code id}. If it was already present
     * it's previous assigned {@code id} will be returned. If it wasn't present
     * it'll be assigned a new {@code id}.
     */
    @Override
    public long add(BytesRef key) {
        final int hash = hash(key);
        return add(key, hash);
    }

    private long add(BytesRef key, int hash) {
        if (smallCore != null) {
            if (size < nextGrowSize) {
                return smallCore.add(key, hash);
            }
            smallCore.transitionToBigCore();
        }
        return bigCore.add(key, hash);
    }

    @Override
    public void add(BytesRef[] keys, long[] ids) {
        final int[] hashes = new int[keys.length];
        Murmur3BytesRefUtils.hashAll(keys, hashes);
        for (int i = 0; i < keys.length; i++) {
            ids[i] = add(keys[i], BitMixer.mix32(hashes[i]));
        }
    }

    @Override
    public Status status() {
        return smallCore != null ? smallCore.status() : bigCore.status();
    }

    public abstract class Itr extends SwissHash.Itr {
        /**
         * The key the iterator current points to.
         */
        public abstract BytesRef key(BytesRef dest);
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
        if (ownsBytesRefs) {
            Releasables.close(bytesRefs);
        }
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
     * Open addressed hash table the probes by triangle numbers. Empty
     * {@code id}s are encoded as {@code -1}. This hash table can't
     * grow, and is instead replaced by a {@link BigCore}.
     *
     * <p> This uses one page from the {@link PageCacheRecycler} for the
     * {@code ids}.
     */
    final class SmallCore extends Core {
        static final float FILL_FACTOR = 0.6F;

        private final byte[] idAndHashPage;

        private SmallCore() {
            boolean success = false;
            try {
                idAndHashPage = grabPage();
                Arrays.fill(idAndHashPage, (byte) 0xff);
                success = true;
            } finally {
                if (success == false) {
                    close();
                }
            }
        }

        int find(final BytesRef key, final int hash) {
            int slot = slot(hash);
            for (;; slot = slot(slot + 1)) {
                long value = (long) LONG_HANDLE.get(idAndHashPage, idAndHashOffset(slot));
                int id = id(value);
                if (id == -1 || (hash(value) == hash && matches(key, id))) {
                    return id;
                }
            }
        }

        int add(final BytesRef key, final int hash) {
            int slot = slot(hash);
            for (;; slot = slot(slot + 1)) {
                final int offset = idAndHashOffset(slot);
                final long value = (long) LONG_HANDLE.get(idAndHashPage, offset);
                final int id = id(value);
                if (id == -1) { // means unset
                    final int nextId = (int) bytesRefs.size();
                    bytesRefs.append(key);
                    final long newValue = ((long) nextId << 32) | Integer.toUnsignedLong(hash);
                    LONG_HANDLE.set(idAndHashPage, offset, newValue);
                    size++;
                    return nextId;
                } else if (hash(value) == hash && matches(key, id)) {
                    return id;
                }
            }
        }

        void transitionToBigCore() {
            int oldCapacity = growTracking();
            try {
                bigCore = new BigCore();
                rehash(oldCapacity);
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
                public BytesRef key(BytesRef dest) {
                    return bytesRefs.get(keyId, dest);
                }
            };
        }

        private void rehash(int oldCapacity) {
            for (int slot = 0; slot < oldCapacity; slot++) {
                final long value = idAndHash(slot);
                final int id = id(value);
                if (id < 0) {
                    continue;
                }
                final int hash = hash(value);
                bigCore.insert(hash, control(hash), id);
            }
        }

        private long idAndHash(int slot) {
            return (long) LONG_HANDLE.get(idAndHashPage, idAndHashOffset(slot));
        }
    }

    /**
     * A SwissHash inspired hashtable. This differs from the normal SwissHash
     * in because it's adapted to Elasticsearch's {@link PageCacheRecycler}.
     * The ids are stored many {@link PageCacheRecycler#PAGE_SIZE_IN_BYTES}
     * arrays.
     */
    final class BigCore extends Core {
        static final float FILL_FACTOR = 0.875F;

        private static final byte EMPTY = (byte) 0x80; // empty slot

        /**
         * The "control" bytes from the SwissHash algorithm.
         */
        private final byte[] controlData;

        /**
         * Pages of {@code ids}, vended by the {@link PageCacheRecycler}. Ids
         * are {@code int}s so it's very quick to select the appropriate page
         * for each slot.
         */
        private final byte[][] idAndHashPages;

        private int insertProbes;

        BigCore() {
            int controlLength = capacity + BYTE_VECTOR_LANES;
            breaker.addEstimateBytesAndMaybeBreak(controlLength, "BytesRefSwissHash-bigCore");
            toClose.add(() -> breaker.addWithoutBreaking(-controlLength));
            controlData = new byte[controlLength];
            Arrays.fill(controlData, EMPTY);

            boolean success = false;
            try {
                int pagesNeeded = (capacity * ID_AND_HASH_SIZE - 1) >> PAGE_SHIFT;
                pagesNeeded++;
                idAndHashPages = new byte[pagesNeeded][];
                for (int i = 0; i < pagesNeeded; i++) {
                    idAndHashPages[i] = grabPage();
                }
                assert idAndHashPages[idAndHashOffset(mask) >> PAGE_SHIFT] != null;
                success = true;
            } finally {
                if (false == success) {
                    close();
                }
            }
        }

        private int find(final BytesRef key, final int hash, final byte control) {
            int group = hash & mask;
            for (;;) {
                ByteVector vec = ByteVector.fromArray(BS, controlData, group);
                long matches = vec.eq(control).toLong();
                while (matches != 0) {
                    final int first = Long.numberOfTrailingZeros(matches);
                    final int checkSlot = slot(group + first);
                    final long value = idAndHash(checkSlot);
                    final int id = id(value);
                    if (hash(value) == hash && matches(key, id)) {
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

        private int add(final BytesRef key, final int hash) {
            maybeGrow();
            return bigCore.addImpl(key, hash);
        }

        private int addImpl(final BytesRef key, final int hash) {
            final byte control = control(hash);
            int group = hash & mask;
            for (;;) {
                ByteVector vec = ByteVector.fromArray(BS, controlData, group);
                long matches = vec.eq(control).toLong();
                while (matches != 0) {
                    final int checkSlot = slot(group + Long.numberOfTrailingZeros(matches));
                    final long value = idAndHash(checkSlot);
                    final int id = id(value);
                    if (hash(value) == hash && matches(key, id)) {
                        return id;
                    }
                    matches &= matches - 1; // clear the first set bit and try again
                }
                long empty = vec.eq(EMPTY).toLong();
                if (empty != 0) {
                    final int insertSlot = slot(group + Long.numberOfTrailingZeros(empty));
                    final int id = (int) bytesRefs.size();
                    bytesRefs.append(key);
                    bigCore.insertAtSlot(insertSlot, hash, control, id);
                    size++;
                    return id;
                }
                group = (group + BYTE_VECTOR_LANES) & mask;
            }
        }

        private void insertAtSlot(final int insertSlot, final int hash, final byte control, final int id) {
            final long value = ((long) id << 32) | Integer.toUnsignedLong(hash);
            final int offset = idAndHashOffset(insertSlot);
            LONG_HANDLE.set(idAndHashPages[offset >> PAGE_SHIFT], offset & PAGE_MASK, value);
            controlData[insertSlot] = control;
            // mirror only if slot is within the first group size, to handle wraparound loads
            if (insertSlot < BYTE_VECTOR_LANES) {
                controlData[insertSlot + capacity] = control;
            }
        }

        @Override
        protected Status status() {
            return new BigCoreStatus(growCount, capacity, size, nextGrowSize, insertProbes, 0, idAndHashPages.length);
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
                public BytesRef key(BytesRef dest) {
                    return bytesRefs.get(keyId, dest);
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
            int oldCapacity = growTracking();
            try {
                BigCore newBigCore = new BigCore();
                rehash(oldCapacity, newBigCore);
                bigCore = newBigCore;
            } finally {
                close();
            }
        }

        private void rehash(int oldCapacity, BigCore newBigCore) {
            for (int i = 0; i < oldCapacity; i++) {
                if (controlData[i] == EMPTY) {
                    continue;
                }
                final long value = idAndHash(i);
                final int hash = hash(value);
                final int id = id(value);
                newBigCore.insert(hash, control(hash), id);
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
                        insertAtSlot(insertSlot, hash, control, id);
                        return;
                    }
                }
                group = (group + BYTE_VECTOR_LANES) & mask;
                insertProbes++;
            }
        }

        private long idAndHash(final int slot) {
            final int offset = idAndHashOffset(slot);
            return (long) LONG_HANDLE.get(idAndHashPages[offset >> PAGE_SHIFT], offset & PAGE_MASK);
        }
    }

    /**
     * Returns the key at <code>0 &lt;= id &lt;= size()</code>.
     * The result is undefined if the id is unused.
     * @param id the id returned when the key was added
     * @return the key
     */
    @Override
    public BytesRef get(long id, BytesRef dest) {
        Objects.checkIndex(id, size());
        return bytesRefs.get(id, dest);
    }

    /** Returns the key array. */
    @Override
    public BytesRefArray getBytesRefs() {
        return bytesRefs;
    }

    int idAndHashOffset(int slot) {
        return slot * ID_AND_HASH_SIZE;
    }

    int id(long value) {
        return (int) (value >>> 32);
    }

    int hash(long value) {
        return (int) value;
    }

    int hash(BytesRef v) {
        return BitMixer.mix32(v.hashCode());
    }

    int slot(int hash) {
        return hash & mask;
    }

    private boolean matches(BytesRef key, int id) {
        return key.bytesEquals(bytesRefs.get(id, scratch));
    }

    @Override
    public long ramBytesUsed() {
        long keys = smallCore != null
            ? smallCore.idAndHashPage.length
            : Arrays.stream(bigCore.idAndHashPages).mapToLong(b -> b.length).sum();
        return BASE_RAM_BYTES_USED + bytesRefs.ramBytesUsed() + keys;
    }
}
