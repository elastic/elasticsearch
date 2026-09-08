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

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.MixHash64;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.PartitionedHashTable;
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
public final class BytesRefSwissHash extends SwissHash implements Accountable, BytesRefHashTable, PartitionedHashTable {

    // base size of the bytes ref hash
    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BytesRefSwissHash.class)
        // spare BytesRef
        + RamUsageEstimator.shallowSizeOfInstance(BytesRef.class) + PagedBytesCursor.SHALLOW_SIZE;

    private static final VectorSpecies<Byte> BS = ByteVector.SPECIES_128;

    private static final int BYTE_VECTOR_LANES = BS.vectorByteSize();

    private static final int PAGE_SHIFT = 14;

    private static final int PAGE_MASK = PageCacheRecycler.PAGE_SIZE_IN_BYTES - 1;

    private static final int ID_AND_HASH_SIZE = Long.BYTES;

    // We use a smaller initial capacity than LongSwissHash because we don't store keys in pages,
    // but we want to be consistent with the page-based sizing logic.
    // PAGE_SIZE / ID_AND_HASH_SIZE = 16384 / 8 = 2048.
    static final int INITIAL_CAPACITY = PageCacheRecycler.PAGE_SIZE_IN_BYTES / ID_AND_HASH_SIZE;

    public static final int DEFAULT_PREFETCH_THRESHOLD = (int) ((1 << 17) * BytesRefSwissHash.BigCore.FILL_FACTOR); // ~114k entries
    public static int PREFETCH_THRESHOLD = DEFAULT_PREFETCH_THRESHOLD;

    /**
     * The recommended size to start partitioning this table. This size is chosen so that the table is large enough to
     * amortize the overhead of partitioning while still fitting in the CPU caches (L3). The ideal value is machine-dependent;
     * this constant is chosen conservatively for a moderate machine.
     */
    public static final int PARTITION_THRESHOLD = 400_000;

    /**
     * Total key bytes at or below which {@link #splitPartition} uses flat (non-paged) storage.
     * Above this threshold it switches to {@link PagedBytesRefPartitionedHashKeys}.
     */
    static final long PAGED_PARTITION_THRESHOLD_BYTES = 400L * 1024 * 1024;

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

    private final BigArrays bigArrays;
    private final BytesRefArray bytesRefs;
    private final boolean ownsBytesRefs;
    private final long pagedPartitionBytesThreshold;
    private final BytesRef scratch = new BytesRef();
    private final PagedBytesCursor cursorScratch = new PagedBytesCursor();

    private SmallCore smallCore;
    private BigCore bigCore;

    /**
     * Creates a new {@link BytesRefSwissHash} that manages its own {@link BytesRefArray}.
     */
    BytesRefSwissHash(PageCacheRecycler recycler, CircuitBreaker breaker, BigArrays bigArrays) {
        this(
            recycler,
            breaker,
            bigArrays,
            new BytesRefArray(PageCacheRecycler.PAGE_SIZE_IN_BYTES, bigArrays),
            true,
            PAGED_PARTITION_THRESHOLD_BYTES
        );
    }

    /**
     * Creates a new {@link BytesRefSwissHash} that manages its own {@link BytesRefArray}, with an
     * explicit threshold controlling when {@link #splitPartition} switches from flat to paged storage.
     * Use this in tests to force one path or the other without allocating large data sets.
     */
    BytesRefSwissHash(PageCacheRecycler recycler, CircuitBreaker breaker, BigArrays bigArrays, long pagedPartitionBytesThreshold) {
        this(
            recycler,
            breaker,
            bigArrays,
            new BytesRefArray(PageCacheRecycler.PAGE_SIZE_IN_BYTES, bigArrays),
            true,
            pagedPartitionBytesThreshold
        );
    }

    /**
     * Creates a new {@link BytesRefSwissHash} that uses the provided {@link BytesRefArray}.
     * This allows multiple {@link BytesRefSwissHash} to share the same key storage and ID space.
     */
    BytesRefSwissHash(PageCacheRecycler recycler, CircuitBreaker breaker, BytesRefArray bytesRefs) {
        this(recycler, breaker, null, bytesRefs, false, PAGED_PARTITION_THRESHOLD_BYTES);
    }

    private BytesRefSwissHash(
        PageCacheRecycler recycler,
        CircuitBreaker breaker,
        BigArrays bigArrays,
        BytesRefArray bytesRefs,
        boolean ownsBytesRefs,
        long pagedPartitionBytesThreshold
    ) {
        super(recycler, breaker, INITIAL_CAPACITY, SmallCore.FILL_FACTOR);
        this.bigArrays = bigArrays;
        this.bytesRefs = bytesRefs;
        this.ownsBytesRefs = ownsBytesRefs;
        this.pagedPartitionBytesThreshold = pagedPartitionBytesThreshold;
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
        final long hash = hash64(key);
        if (smallCore != null) {
            return smallCore.find(key, hash);
        } else {
            return bigCore.find(key, hash);
        }
    }

    /**
     * Whether the hash table is large enough for prefetch to be useful
     */
    public boolean shouldPrefetch() {
        return size >= PREFETCH_THRESHOLD && bigCore != null;
    }

    /**
     * Prefetch the data at the slot of the given hash. The caller should only call this method
     * when {@link #shouldPrefetch()} return true.
     */
    public int prefetch(long hash) {
        return bigCore.prefetch(hash);
    }

    /**
     * Finds an {@code id} by a {@code key}.
     */
    public long find(PagedBytesCursor key) {
        final long hash = hash64(key);
        if (smallCore != null) {
            return smallCore.find(key, hash);
        } else {
            return bigCore.find(key, hash);
        }
    }

    /**
     * Adds a {@code key}, returning its {@code id}. If it was already present
     * it's previous assigned {@code id} will be returned. If it wasn't present
     * it'll be assigned a new {@code id}.
     */
    @Override
    public long add(BytesRef key) {
        final long hash = hash64(key);
        return addWithHash(key, hash);
    }

    /**
     * Same semantic as {@link #add(BytesRef)} but accepts a pre-computed hash.
     */
    public int addWithHash(BytesRef key, long hash) {
        if (smallCore != null) {
            if (size < nextGrowSize) {
                return smallCore.add(key, hash);
            }
            smallCore.transitionToBigCore();
        }
        return bigCore.addWithHash(key, hash);
    }

    /**
     * Adds a {@code key}, returning its {@code id}. If it was already present
     * it's previous assigned {@code id} will be returned. If it wasn't present
     * it'll be assigned a new {@code id}. The cursor is drained (advanced to
     * its end) when a new key is inserted.
     */
    @Override
    public long add(PagedBytesCursor key) {
        final long hash = hash64(key);
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
    private static byte control(long hash) {
        return (byte) (hash >>> (Long.SIZE - 7));
    }

    /**
     * Returns the partition index using bits that do not overlap with the lower 32 hash-slot bits or the top 7 control bits.
     */
    private static int partition(long hash64) {
        return (int) (hash64 >>> Integer.SIZE) & PARTITION_MASK;
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
     */
    final class SmallCore extends Core {
        static final float FILL_FACTOR = 0.6F;

        private final long[] slots;
        private final byte[] controlData; // stored for rehash

        private SmallCore() {
            final long requiredBytes = (long) capacity * Long.BYTES + capacity;
            breaker.addEstimateBytesAndMaybeBreak(requiredBytes, "BytesRefSwissHash-smallCore");
            usedBytes += requiredBytes;
            slots = new long[capacity];
            controlData = new byte[capacity];
            Arrays.fill(slots, -1L);
        }

        /**
         * Find bytes in the hash. This has a lot of duplication with {@link #find(PagedBytesCursor, long)}
         * but we're intentionally doing it to make them look exactly the same. And because this
         * is the hottest of the hot path.
         */
        int find(final BytesRef key, final long hash) {
            int slot = slot((int) hash);
            for (;; slot = slot(slot + 1)) {
                final long packed = slots[slot];
                final int id = (int) (packed >>> 32);
                if (id == -1 || ((int) packed == (int) hash && matches(key, id))) {
                    return id;
                }
            }
        }

        /**
         * Find bytes in the hash. This has a lot of duplication with {@link #find(BytesRef, long)}
         * but we're intentionally doing it to make them look exactly the same. And because this
         * is the hottest of the hot path.
         */
        int find(final PagedBytesCursor key, final long hash) {
            int slot = slot((int) hash);
            for (;; slot = slot(slot + 1)) {
                final long packed = slots[slot];
                final int id = (int) (packed >>> 32);
                if (id == -1 || ((int) packed == (int) hash && matches(key, id))) {
                    return id;
                }
            }
        }

        /**
         * Adds to the hash. This has a lot of duplication with {@link #add(PagedBytesCursor, long)}
         * but we're intentionally doing it to make them look exactly the same. And because this
         * is the hottest of the hot path.
         */
        int add(final BytesRef key, final long hash) {
            int slot = slot((int) hash);
            for (;; slot = slot(slot + 1)) {
                final long packed = slots[slot];
                final int id = (int) (packed >>> 32);
                if (id == -1) {
                    final int nextId = (int) bytesRefs.size();
                    bytesRefs.append(key);
                    slots[slot] = ((long) nextId << 32) | Integer.toUnsignedLong((int) hash);
                    controlData[slot] = control(hash);
                    size++;
                    return nextId;
                } else if ((int) packed == (int) hash && matches(key, id)) {
                    return -1 - id;
                }
            }
        }

        /**
         * Adds to the hash. This has a lot of duplication with {@link #add(BytesRef, long)}
         * but we're intentionally doing it to make them look exactly the same. And because this
         * is the hottest of the hot path.
         */
        int add(final PagedBytesCursor key, final long hash) {
            int slot = slot((int) hash);
            for (;; slot = slot(slot + 1)) {
                final long packed = slots[slot];
                final int id = (int) (packed >>> 32);
                if (id == -1) {
                    final int nextId = (int) bytesRefs.size();
                    bytesRefs.append(key);
                    slots[slot] = ((long) nextId << 32) | Integer.toUnsignedLong((int) hash);
                    controlData[slot] = control(hash);
                    size++;
                    return nextId;
                } else if ((int) packed == (int) hash && matches(key, id)) {
                    return -1 - id;
                }
            }
        }

        /**
         * Returns {@code true} if every key was appended, allowing the caller to apply append-only optimizations.
         */
        boolean mergeKeys(BytesRefArray keys, int len, int[] ids) {
            final BytesRef key = new BytesRef();
            final int preSize = size;
            for (int i = 0; i < len; i++) {
                keys.get(i, key);
                final long hash = hash64(key);
                final int id = add(key, hash);
                ids[i] = id >= 0 ? id : -1 - id;
            }
            return size == preSize + len;
        }

        boolean mergeKeys(byte[] data, int[] offsets, int len, int[] ids) {
            final BytesRef key = new BytesRef();
            key.bytes = data;
            final int preSize = size;
            for (int i = 0; i < len; i++) {
                key.offset = offsets[i];
                key.length = offsets[i + 1] - offsets[i];
                final long hash = hash64(key);
                final int id = add(key, hash);
                ids[i] = id >= 0 ? id : -1 - id;
            }
            return size == preSize + len;
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

        void clear() {
            Arrays.fill(slots, -1L);
            Arrays.fill(controlData, (byte) 0);
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
                final long packed = slots[slot];
                final int id = (int) (packed >>> 32);
                if (id < 0) {
                    continue;
                }
                bigCore.insert((int) packed, controlData[slot], id);
            }
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

        /**
         * Find bytes in the hash. This has a lot of duplication with {@link #find(PagedBytesCursor, long)}
         * but we're intentionally doing it to make them look exactly the same. And because this
         * is the hottest of the hot path.
         */
        private int find(final BytesRef key, final long hash64) {
            final int hash = hash(hash64);
            final byte control = control(hash64);
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

        /**
         * Find bytes in the hash. This has a lot of duplication with {@link #find(BytesRef, long)}
         * but we're intentionally doing it to make them look exactly the same. And because this
         * is the hottest of the hot path.
         */
        private int find(final PagedBytesCursor key, final long hash64) {
            final int hash = hash(hash64);
            final byte control = control(hash64);
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

        int prefetch(long hash64) {
            final int group = hash(hash64) & mask;
            final int idOff = idAndHashOffset(group);
            return controlData[group] ^ idAndHashPages[idOff >> PAGE_SHIFT][idOff & PAGE_MASK];
        }

        private int addWithHash(final BytesRef key, final long hash) {
            maybeGrow();
            return bigCore.addImpl(key, hash);
        }

        /**
         * Adds to the hash. This has a lot of duplication with {@link #addImpl(PagedBytesCursor, long)}
         * but we're intentionally doing it to make them look exactly the same. And because this
         * is the hottest of the hot path.
         */
        private int addImpl(final BytesRef key, final long hash64) {
            final int hash = hash(hash64);
            final byte control = control(hash64);
            int group = hash & mask;
            for (;;) {
                ByteVector vec = ByteVector.fromArray(BS, controlData, group);
                long matches = vec.eq(control).toLong();
                while (matches != 0) {
                    final int checkSlot = slot(group + Long.numberOfTrailingZeros(matches));
                    final long value = idAndHash(checkSlot);
                    final int id = id(value);
                    if (hash(value) == hash && matches(key, id)) {
                        return -1 - id;
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

        private int add(final PagedBytesCursor key, final long hash64) {
            maybeGrow();
            return bigCore.addImpl(key, hash64);
        }

        /**
         * Adds to the hash. This has a lot of duplication with {@link #addImpl(BytesRef, long)}
         * but we're intentionally doing it to make them look exactly the same. And because this
         * is the hottest of the hot path.
         */
        private int addImpl(final PagedBytesCursor key, final long hash64) {
            final int hash = hash(hash64);
            final byte control = control(hash64);
            int group = hash & mask;
            for (;;) {
                ByteVector vec = ByteVector.fromArray(BS, controlData, group);
                long matches = vec.eq(control).toLong();
                while (matches != 0) {
                    final int checkSlot = slot(group + Long.numberOfTrailingZeros(matches));
                    final long value = idAndHash(checkSlot);
                    final int id = id(value);
                    if (hash(value) == hash && matches(key, id)) {
                        return -1 - id;
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

        void grow(int minSize) {
            do {
                growTracking();
            } while (nextGrowSize < minSize);
            int oldCapacity = controlData.length - BYTE_VECTOR_LANES;
            try {
                BigCore newBigCore = new BigCore();
                rehash(oldCapacity, newBigCore);
                bigCore = newBigCore;
            } finally {
                close();
            }
        }

        /**
         * Returns {@code true} if every key was appended, allowing the caller to apply append-only optimizations.
         */
        boolean mergeKeys(BytesRefArray keys, int len, int[] ids) {
            final BytesRef key = new BytesRef();
            final int preSize = size;
            if (preSize == 0) {
                for (int i = 0; i < len; i++) {
                    keys.get(i, key);
                    final long hash64 = hash64(key);
                    final int id = (int) bytesRefs.size();
                    insert(hash(hash64), control(hash64), id);
                    bytesRefs.append(key);
                    ids[i] = id;
                }
                size = len;
                return true;
            }
            for (int i = 0; i < len; i++) {
                keys.get(i, key);
                final long hash64 = hash64(key);
                final int id = addImpl(key, hash64);
                ids[i] = id >= 0 ? id : -1 - id;
            }
            return size == preSize + len;
        }

        boolean mergeKeys(byte[] data, int[] offsets, int len, int[] ids) {
            final BytesRef key = new BytesRef();
            key.bytes = data;
            final int preSize = size;
            if (preSize == 0) {
                for (int id = 0; id < len; id++) {
                    key.offset = offsets[id];
                    key.length = offsets[id + 1] - offsets[id];
                    final long hash64 = hash64(key);
                    insert(hash(hash64), control(hash64), (int) bytesRefs.size());
                    bytesRefs.append(key);
                    ids[id] = id;
                }
                size = len;
                return true;
            }
            for (int i = 0; i < len; i++) {
                key.offset = offsets[i];
                key.length = offsets[i + 1] - offsets[i];
                final long hash64 = hash64(key);
                final int id = addImpl(key, hash64);
                ids[i] = id >= 0 ? id : -1 - id;
            }
            return size == preSize + len;
        }

        void clear() {
            Arrays.fill(controlData, EMPTY);
            insertProbes = 0;
        }

        private void rehash(int oldCapacity, BigCore newBigCore) {
            for (int i = 0; i < oldCapacity; i++) {
                final byte control = controlData[i];
                if (control == EMPTY) {
                    continue;
                }
                final long value = idAndHash(i);
                final int hash = hash(value);
                final int id = id(value);
                newBigCore.insert(hash, control, id);
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

    private static int hash(long value) {
        return (int) value;
    }

    public static long hash64(BytesRef v) {
        return MixHash64.hash64(v);
    }

    public static long hash64(byte[] bytes, int offset, int length) {
        return MixHash64.hash64(bytes, offset, length);
    }

    static long hash64(PagedBytesCursor cursor) {
        return cursor.mixHash64();
    }

    int slot(int hash) {
        return hash & mask;
    }

    private boolean matches(BytesRef key, int id) {
        return bytesRefs.bytesEqual(id, key);
    }

    private boolean matches(PagedBytesCursor key, int id) {
        return key.equals(bytesRefs.get(id, cursorScratch));
    }

    /**
     * Grows this table so it can hold at least {@code minSize} entries without future resizing.
     */
    public void ensureCapacity(int minSize) {
        if (nextGrowSize < minSize) {
            if (smallCore != null) {
                smallCore.transitionToBigCore();
                if (nextGrowSize < minSize) {
                    bigCore.grow(minSize);
                }
            } else {
                bigCore.grow(minSize);
            }
        }
    }

    /**
     * Removes all entries while retaining the allocated capacity, so the table can be refilled without resizing.
     */
    public void clear() {
        size = 0;
        growCount = 0;
        if (ownsBytesRefs) {
            bytesRefs.truncateTo(0);
        }
        if (smallCore != null) {
            smallCore.clear();
        } else {
            bigCore.clear();
        }
    }

    @Override
    public PartitionedHashKeys splitPartition(CircuitBreaker breaker, PartitionSplitter partitionSplitter) {
        final int[] batchPartitionCounts = new int[NUM_PARTITIONS];
        final short[] shiftedIds = new short[PARTITION_WRITE_BATCH * NUM_PARTITIONS];
        int batchStart = 0;
        assert ownsBytesRefs : "splitPartition is only valid when this hash owns its BytesRefArray; ids are non-consecutive when shared";
        final long totalKeyBytes = bytesRefs.totalBytes();
        final BytesRefPartitionedHashKeys partitionedKeys = totalKeyBytes <= pagedPartitionBytesThreshold
            ? new FlatBytesRefPartitionedHashKeys(breaker, size, totalKeyBytes)
            : new PagedBytesRefPartitionedHashKeys(bigArrays, size, totalKeyBytes);
        final int[] partitionOffsets = partitionedKeys.partitionCounts;
        boolean success = false;
        try {
            for (int id = 0; id < size; id++) {
                bytesRefs.get(id, scratch);
                final long hash64 = hash64(scratch);
                final int p = partition(hash64);
                if (batchPartitionCounts[p] == PARTITION_WRITE_BATCH) {
                    partitionedKeys.splitKeys(breaker, bytesRefs, scratch, batchStart, shiftedIds, batchPartitionCounts);
                    partitionSplitter.split(batchStart, shiftedIds, id - batchStart, batchPartitionCounts, partitionOffsets);
                    for (int i = 0; i < NUM_PARTITIONS; i++) {
                        partitionOffsets[i] += batchPartitionCounts[i];
                    }
                    batchStart = id;
                    Arrays.fill(batchPartitionCounts, 0);
                }
                int c = batchPartitionCounts[p]++;
                assert id - batchStart <= Short.MAX_VALUE : id - batchStart;
                shiftedIds[p * PARTITION_WRITE_BATCH + c] = (short) (id - batchStart);
            }
            partitionedKeys.splitKeys(breaker, bytesRefs, scratch, batchStart, shiftedIds, batchPartitionCounts);
            partitionSplitter.split(batchStart, shiftedIds, size - batchStart, batchPartitionCounts, partitionOffsets);
            for (int i = 0; i < NUM_PARTITIONS; i++) {
                partitionOffsets[i] += batchPartitionCounts[i];
            }
            success = true;
        } finally {
            if (success == false) {
                partitionedKeys.releaseAll(breaker);
            }
        }
        return partitionedKeys;
    }

    @Override
    public boolean combinePartition(PartitionedHashKeys keys, int partitionIndex, int[] resultIds) {
        Objects.requireNonNull(resultIds);
        final int keysInPartition = keys.keysInPartition(partitionIndex);
        assert keysInPartition <= resultIds.length : keysInPartition + " > " + resultIds.length;
        final int needed = Math.addExact(size, keysInPartition);
        ensureCapacity(needed);
        if (keys instanceof FlatBytesRefPartitionedHashKeys flat) {
            assert flat.partitionData[partitionIndex] != null : "partition [" + partitionIndex + "] was already released";
            if (smallCore != null) {
                return smallCore.mergeKeys(
                    flat.partitionData[partitionIndex],
                    flat.partitionOffsets[partitionIndex],
                    keysInPartition,
                    resultIds
                );
            } else {
                return bigCore.mergeKeys(
                    flat.partitionData[partitionIndex],
                    flat.partitionOffsets[partitionIndex],
                    keysInPartition,
                    resultIds
                );
            }
        } else {
            final PagedBytesRefPartitionedHashKeys paged = (PagedBytesRefPartitionedHashKeys) keys;
            assert paged.partitionArrays[partitionIndex] != null : "partition [" + partitionIndex + "] was already released";
            if (smallCore != null) {
                return smallCore.mergeKeys(paged.partitionArrays[partitionIndex], keysInPartition, resultIds);
            } else {
                return bigCore.mergeKeys(paged.partitionArrays[partitionIndex], keysInPartition, resultIds);
            }
        }
    }

    abstract static sealed class BytesRefPartitionedHashKeys implements PartitionedHashKeys permits FlatBytesRefPartitionedHashKeys,
        PagedBytesRefPartitionedHashKeys {

        final int[] partitionCounts = new int[NUM_PARTITIONS];

        @Override
        public int keysInPartition(int partition) {
            return partitionCounts[partition];
        }

        abstract void splitKeys(
            CircuitBreaker breaker,
            BytesRefArray bytesRefs,
            BytesRef scratch,
            int idOffset,
            short[] positions,
            int[] fills
        );
    }

    static final class FlatBytesRefPartitionedHashKeys extends BytesRefPartitionedHashKeys {
        final int[] partitionDataUsed;
        byte[][] partitionData;
        int[][] partitionOffsets;

        FlatBytesRefPartitionedHashKeys(CircuitBreaker breaker, int totalKeys, long totalKeyBytes) {
            final int avgKeysPerPartition = Math.max(Math.ceilDiv(totalKeys, NUM_PARTITIONS), 1);
            final int avgBytesPerPartition = (int) Math.ceilDiv(totalKeyBytes, NUM_PARTITIONS);
            final int initialBytes = ArrayUtil.oversize(avgBytesPerPartition, 1);
            final int initialOffsets = ArrayUtil.oversize(avgKeysPerPartition + 1, Integer.BYTES);
            long usedBytes = (long) NUM_PARTITIONS * Integer.BYTES + (long) NUM_PARTITIONS * initialBytes + (long) NUM_PARTITIONS
                * initialOffsets * Integer.BYTES;
            breaker.addEstimateBytesAndMaybeBreak(usedBytes, "BytesRefSwissHash#partition");
            partitionDataUsed = new int[NUM_PARTITIONS];
            partitionData = new byte[NUM_PARTITIONS][];
            partitionOffsets = new int[NUM_PARTITIONS][];
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                partitionData[p] = new byte[initialBytes];
                partitionOffsets[p] = new int[initialOffsets];
            }
        }

        @Override
        void splitKeys(CircuitBreaker breaker, BytesRefArray bytesRefs, BytesRef scratch, int idOffset, short[] positions, int[] fills) {
            assert NUM_PARTITIONS * PARTITION_WRITE_BATCH < Short.MAX_VALUE : "shifted ids of one batch must fit in the short value range";
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                final int c = fills[p];
                if (c == 0) {
                    continue;
                }
                final int base = p * PARTITION_WRITE_BATCH;
                final int keyBase = partitionCounts[p];
                ensureOffsetCapacity(breaker, p, keyBase + c + 1);
                for (int i = 0; i < c; i++) {
                    final int id = idOffset + (positions[base + i] & 0xFFFF);
                    bytesRefs.get(id, scratch);
                    ensureDataCapacity(breaker, p, partitionDataUsed[p] + scratch.length);
                    partitionOffsets[p][keyBase + i] = partitionDataUsed[p];
                    System.arraycopy(scratch.bytes, scratch.offset, partitionData[p], partitionDataUsed[p], scratch.length);
                    partitionDataUsed[p] += scratch.length;
                }
                partitionOffsets[p][keyBase + c] = partitionDataUsed[p];
            }
        }

        private void ensureDataCapacity(CircuitBreaker breaker, int p, int minLength) {
            final byte[] sub = partitionData[p];
            if (sub.length >= minLength) {
                return;
            }
            final int newLength = ArrayUtil.oversize(minLength, 1);
            breaker.addEstimateBytesAndMaybeBreak(newLength, "BytesRefSwissHash#partition");
            partitionData[p] = Arrays.copyOf(sub, newLength);
            breaker.addWithoutBreaking(-sub.length);
        }

        private void ensureOffsetCapacity(CircuitBreaker breaker, int p, int minCount) {
            final int[] sub = partitionOffsets[p];
            if (sub.length >= minCount) {
                return;
            }
            final int newCount = ArrayUtil.oversize(minCount, Integer.BYTES);
            breaker.addEstimateBytesAndMaybeBreak((long) newCount * Integer.BYTES, "BytesRefSwissHash#partition");
            partitionOffsets[p] = Arrays.copyOf(sub, newCount);
            breaker.addWithoutBreaking(-(long) sub.length * Integer.BYTES);
        }

        @Override
        public void releasePartition(CircuitBreaker breaker, int partition) {
            final byte[] data = partitionData[partition];
            if (data != null) {
                final int[] offsets = partitionOffsets[partition];
                partitionData[partition] = null;
                partitionOffsets[partition] = null;
                breaker.addWithoutBreaking(-data.length - (long) offsets.length * Integer.BYTES);
            }
        }

        @Override
        public void releaseAll(CircuitBreaker breaker) {
            long bytes = (long) NUM_PARTITIONS * Integer.BYTES;
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                final byte[] data = partitionData[p];
                if (data != null) {
                    bytes += data.length;
                }
                final int[] offsets = partitionOffsets[p];
                if (offsets != null) {
                    bytes += (long) offsets.length * Integer.BYTES;
                }
            }
            partitionData = null;
            partitionOffsets = null;
            breaker.addWithoutBreaking(-bytes);
        }
    }

    static final class PagedBytesRefPartitionedHashKeys extends BytesRefPartitionedHashKeys {
        BytesRefArray[] partitionArrays;

        PagedBytesRefPartitionedHashKeys(BigArrays bigArrays, int totalKeys, long totalKeyBytes) {
            final int avgKeysPerPartition = Math.max(Math.ceilDiv(totalKeys, NUM_PARTITIONS), 1);
            final int avgBytesPerPartition = (int) Math.ceilDiv(totalKeyBytes, NUM_PARTITIONS);
            partitionArrays = new BytesRefArray[NUM_PARTITIONS];
            boolean success = false;
            try {
                for (int p = 0; p < NUM_PARTITIONS; p++) {
                    partitionArrays[p] = new BytesRefArray(avgKeysPerPartition, bigArrays, avgBytesPerPartition);
                }
                success = true;
            } finally {
                if (success == false) {
                    Releasables.close(partitionArrays);
                }
            }
        }

        @Override
        void splitKeys(CircuitBreaker breaker, BytesRefArray bytesRefs, BytesRef scratch, int idOffset, short[] positions, int[] fills) {
            assert NUM_PARTITIONS * PARTITION_WRITE_BATCH < Short.MAX_VALUE : "shifted ids of one batch must fit in the short value range";
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                final int c = fills[p];
                if (c == 0) {
                    continue;
                }
                final int base = p * PARTITION_WRITE_BATCH;
                for (int i = 0; i < c; i++) {
                    final int id = idOffset + (positions[base + i] & 0xFFFF);
                    bytesRefs.get(id, scratch);
                    partitionArrays[p].append(scratch);
                }
            }
        }

        @Override
        public void releasePartition(CircuitBreaker breaker, int partition) {
            if (partitionArrays[partition] != null) {
                partitionArrays[partition].close();
                partitionArrays[partition] = null;
            }
        }

        @Override
        public void releaseAll(CircuitBreaker breaker) {
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                if (partitionArrays[p] != null) {
                    partitionArrays[p].close();
                    partitionArrays[p] = null;
                }
            }
        }
    }

    @Override
    public long ramBytesUsed() {
        long keys = smallCore != null
            ? (long) smallCore.slots.length * Long.BYTES + smallCore.controlData.length
            : Arrays.stream(bigCore.idAndHashPages).mapToLong(b -> b.length).sum();
        return BASE_RAM_BYTES_USED + bytesRefs.ramBytesUsed() + keys;
    }
}
