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
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.LongLongHashTable;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.PartitionedHashTable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Specialization for LongSwissHash, for LongLong. */
public class LongLongSwissHash extends SwissHash implements LongLongHashTable, PartitionedHashTable {

    static final VectorSpecies<Byte> BS = ByteVector.SPECIES_128;

    private static final int BYTE_VECTOR_LANES = BS.vectorByteSize();

    private static final int PAGE_SHIFT = 14;

    private static final int PAGE_MASK = PageCacheRecycler.PAGE_SIZE_IN_BYTES - 1;

    private static final int KEY_SIZE = Long.BYTES + Long.BYTES;

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

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(LongLongSwissHash.class);

    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());

    /**
     * The default threshold above which bulk-add is used. It is based on the capacity of the table, not the number
     * of entries: once the capacity is this large, the memory used by the controls, ids, and hashes exceeds the
     * CPU caches (L2), so the prefetching done by bulk-add outweighs its overhead.
     */
    static final int DEFAULT_BULK_THRESHOLD = (int) ((1 << 17) * BigCore.FILL_FACTOR); // ~114k entries in the capacity

    /**
     * The recommended size to start partitioning this table. This size is chosen so that the table is large enough to
     * amortize the overhead of partitioning while still fitting in the CPU caches (L3). The ideal value is machine-dependent;
     * this constant is chosen conservatively for a moderate machine.
     */
    public static final int PARTITION_THRESHOLD = 400_000;

    /**
     * Pages of {@code keys}, vended by the {@link PageCacheRecycler}. It's
     * important that the size of keys be a power of two, so we can quickly
     * select the appropriate page and keys never span multiple pages.
     */
    private byte[][] keyPages;
    private SmallCore smallCore;
    private BigCore bigCore;
    private long usedBytesByKeyPages = 0;
    private final int bulkThreshold;
    private final List<Releasable> toClose = new ArrayList<>();

    LongLongSwissHash(PageCacheRecycler recycler, CircuitBreaker breaker) {
        this(recycler, breaker, DEFAULT_BULK_THRESHOLD);
    }

    LongLongSwissHash(PageCacheRecycler recycler, CircuitBreaker breaker, int bulkThreshold) {
        super(recycler, breaker, INITIAL_CAPACITY, LongSwissHash.SmallCore.FILL_FACTOR);
        this.bulkThreshold = bulkThreshold;
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
        breaker.addEstimateBytesAndMaybeBreak(PageCacheRecycler.PAGE_SIZE_IN_BYTES, "LongLongSwissHash-keyPage");
        usedBytesByKeyPages += PageCacheRecycler.PAGE_SIZE_IN_BYTES;
        Recycler.V<byte[]> page = recycler.bytePage(false);
        toClose.add(page);
        return page.v();
    }

    /**
     * Finds an {@code id} by a {@code key1} and a {@code key2}.
     */
    @Override
    public long find(final long key1, final long key2) {
        final long hash = hash(key1, key2);
        if (smallCore != null) {
            return smallCore.find(key1, key2, hash);
        } else {
            return bigCore.find(key1, key2, hash, control(hash));
        }
    }

    /**
     * Add a {@code key}, returning its {@code id}s. If it was already present
     * it's previous assigned {@code id} will be returned. If it wasn't present
     * it'll be assigned a new {@code id}.
     */
    @Override
    public long add(final long key1, final long key2) {
        final long hash = hash(key1, key2);
        if (smallCore != null) {
            if (size < nextGrowSize) {
                return smallCore.add(key1, key2, hash);
            }
            smallCore.transitionToBigCore(nextGrowSize);
        }
        return bigCore.add(key1, key2, hash);
    }

    @Override
    public boolean supportBulkAdd() {
        return bigCore != null && nextGrowSize > bulkThreshold;
    }

    @Override
    public void bulkAdd(long[] key1s, long[] key2s, int[] ids, int length) {
        assert bigCore != null : "must call supportBulkAdd before";
        final long needed = (long) size + length;
        if (nextGrowSize <= needed) {
            bigCore.grow(Math.toIntExact(needed + 1));
        }
        bigCore.batchAdd(key1s, key2s, ids, length);
    }

    @Override
    public Status status() {
        return smallCore != null ? smallCore.status() : bigCore.status();
    }

    public abstract class Itr extends SwissHash.Itr {
        /** The first key the iterator current points to. */
        public abstract long key1();

        /** The second key the iterator current points to. */
        public abstract long key2();
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
     * Lower 32 bits of the hash, stored alongside the id to enable rehash without re-hashing keys
     * and to serve as a secondary filter in probes.
     */
    private static int storedHash(long hash) {
        return (int) hash;
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
        Releasables.close(toClose);
        toClose.clear();
        breaker.addWithoutBreaking(-usedBytesByKeyPages);
        usedBytesByKeyPages = 0;
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
     * Open addressed hash table with linear probing. Empty {@code id}s are
     * encoded as {@code -1}. This hash table can't grow, and is instead
     * replaced by a {@link LongLongSwissHash.BigCore}.
     *
     * <p> This uses one page from the {@link PageCacheRecycler} for the
     * {@code ids}.
     */
    final class SmallCore extends Core implements Accountable {
        static final float FILL_FACTOR = 0.6F;
        static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(SmallCore.class);

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

        int find(final long key1, final long key2, final long hash) {
            int slot = slot(hash);
            for (;;) {
                int id = id(slot);
                if (id < 0) {
                    return -1; // empty
                }
                final int offset = keyOffset(id);
                if (key1(offset) == key1 && key2(offset) == key2) {
                    return id;
                }
                slot = slot(slot + 1);
            }
        }

        int add(final long key1, final long key2, final long hash) {
            int slot = slot(hash);
            for (;;) {
                final int idOffset = idOffset(slot);
                final int currentId = (int) INT_HANDLE.get(idPage, idOffset);
                if (currentId >= 0) {
                    final int keyOffset = keyOffset(currentId);
                    if (key1(keyOffset) == key1 && key2(keyOffset) == key2) {
                        return -1 - currentId;
                    }
                    slot = slot(slot + 1);
                } else {
                    int id = size;
                    final int keyOffset = keyOffset(id);
                    INT_HANDLE.set(idPage, idOffset, id);
                    setKeys(keyOffset, key1, key2);
                    size++;
                    return id;
                }
            }
        }

        /**
         * Returns {@code true} if every key was appended, allowing the caller to apply append-only optimizations.
         */
        boolean mergeKeys(long[] keys, int[] ids, int len) {
            Objects.checkFromIndexSize(0, len, keys.length / 2);
            Objects.checkFromIndexSize(0, len, ids.length);

            final int preSize = size;
            for (int i = 0; i < len; i++) {
                long k1 = keys[i * 2];
                long k2 = keys[i * 2 + 1];
                long hash = hash(k1, k2);
                int id = add(k1, k2, hash);
                ids[i] = id >= 0 ? id : -1 - id;
            }
            return size == preSize + len;
        }

        void transitionToBigCore(int minSize) {
            do {
                growTracking();
            } while (nextGrowSize < minSize);
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
        protected LongLongSwissHash.Itr iterator() {
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
                public long key1() {
                    return SmallCore.this.key1(keyOffset(keyId));
                }

                @Override
                public long key2() {
                    return SmallCore.this.key2(keyOffset(keyId));
                }
            };
        }

        private void rehash() {
            for (int i = 0; i < size; i++) {
                final int keyOffset = keyOffset(i);
                final long hash64 = hash(key1(keyOffset), key2(keyOffset));
                bigCore.insert((int) hash64, control(hash64), i);
            }
        }

        private long key1(int offset) {
            return (long) LONG_HANDLE.get(keyPages[0], offset);
        }

        private long key2(int offset) {
            return (long) LONG_HANDLE.get(keyPages[0], offset + Long.BYTES);
        }

        private void setKeys(int offset, long value1, long value2) {
            LONG_HANDLE.set(keyPages[0], offset, value1);
            LONG_HANDLE.set(keyPages[0], offset + Long.BYTES, value2);
        }

        private int keyOffset(final int id) {
            return id * KEY_SIZE;
        }

        private int id(int slot) {
            return (int) INT_HANDLE.get(idPage, idOffset(slot));
        }

        private int idOffset(final int slot) {
            return slot * ID_SIZE;
        }

        @Override
        public long ramBytesUsed() {
            return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(idPage);
        }

        void clear() {
            Arrays.fill(idPage, (byte) 0xff);
        }
    }

    final class BigCore extends Core implements Accountable {
        static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BigCore.class);

        static final float FILL_FACTOR = 0.875F;

        private static final byte EMPTY = (byte) 0x80; // empty slot
        private static final int ID_AND_HASH = Integer.BYTES + Integer.BYTES;
        private static final int ID_PAGE_SHIFT = PAGE_SHIFT - 3;

        private static final VarHandle SINK_HANDLE;
        static {
            try {
                SINK_HANDLE = MethodHandles.lookup().findVarHandle(BigCore.class, "sinkHandle", long.class);
            } catch (Exception e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        private final byte[] controlData;
        private final byte[][] idPages;
        private int insertProbes;
        private long sinkHandle;

        BigCore() {
            final int controlLength = capacity + BYTE_VECTOR_LANES;
            breaker.addEstimateBytesAndMaybeBreak(controlLength, "LongLongSwissHash-bigCore");
            toClose.add(() -> breaker.addWithoutBreaking(-controlLength));
            controlData = new byte[controlLength];
            Arrays.fill(controlData, EMPTY);

            boolean success = false;
            try {
                final int keyPagesNeeded = requiredPages(nextGrowSize, KEY_SIZE);
                var initialKeyPages = keyPages;
                keyPages = new byte[keyPagesNeeded][];
                for (int i = 0; i < keyPagesNeeded; i++) {
                    keyPages[i] = (i < initialKeyPages.length) ? initialKeyPages[i] : grabKeyPage();
                }
                int idPagesNeeded = requiredPages(capacity, ID_AND_HASH);
                idPages = new byte[idPagesNeeded][];
                for (int i = 0; i < idPagesNeeded; i++) {
                    idPages[i] = grabPage();
                }
                assert idPages[(int) (((long) mask * ID_AND_HASH) >> PAGE_SHIFT)] != null;
                success = true;
            } finally {
                if (false == success) {
                    close();
                }
            }
        }

        private static int requiredPages(long numElements, int elementSize) {
            final int elementsPerPage = PageCacheRecycler.PAGE_SIZE_IN_BYTES / elementSize;
            return Math.toIntExact((numElements + elementsPerPage - 1) / elementsPerPage);
        }

        private int find(final long key1, final long key2, final long hash, final byte control) {
            final int storedHash = storedHash(hash);
            int group = (int) hash & mask;
            for (;;) {
                final ByteVector vec = ByteVector.fromArray(BS, controlData, group);
                long matches = vec.eq(control).toLong();
                while (matches != 0) {
                    final int checkSlot = (group + Long.numberOfTrailingZeros(matches)) & mask;
                    final long packed = idAndHash(checkSlot);
                    if ((int) packed == storedHash) {
                        final int id = (int) (packed >>> 32);
                        if (equalKeys(keyOffset(id), key1, key2)) {
                            return id;
                        }
                    }
                    matches &= matches - 1; // clear the first set bit and try again
                }
                long empty = vec.eq(EMPTY).toLong();
                if (empty != 0) {
                    return -1;
                }
                group = (group + BYTE_VECTOR_LANES) & mask;
            }
        }

        private int add(final long key1, final long key2, final long hash) {
            if (size >= nextGrowSize) {
                assert size == nextGrowSize;
                grow(size);
            }
            return bigCore.addImpl(key1, key2, hash);
        }

        private static final int CHUNK_SIZE = 128;
        private final long[] batchHashes = new long[CHUNK_SIZE];
        private final byte[][] batchPagesRefs = new byte[CHUNK_SIZE][];

        private void batchAdd(long[] key1s, long[] key2s, int[] ids, int length) {
            int offset = 0;
            long dummy = 0;
            while (offset < length) {
                final int chunkSize = Math.min(length - offset, CHUNK_SIZE);
                // compute hashes and fetch id page refs
                for (int i = 0; i < chunkSize; i++) {
                    final int absIdx = offset + i;
                    final long hash = hash(key1s[absIdx], key2s[absIdx]);
                    batchHashes[i] = hash;
                    batchPagesRefs[i] = idPages[((int) hash & mask) >> ID_PAGE_SHIFT];
                }
                // touch controls and id-hash data to warm caches
                for (int i = 0; i < chunkSize; i++) {
                    final int group = ((int) batchHashes[i]) & mask;
                    dummy ^= controlData[group];
                    dummy ^= batchPagesRefs[i][idOffset(group) & PAGE_MASK];
                }
                // insert using pre-computed hashes
                for (int r = 0; r < chunkSize; r++) {
                    final int id = addImpl(key1s[offset + r], key2s[offset + r], batchHashes[r]);
                    ids[offset + r] = id >= 0 ? id : -1 - id;
                }
                offset += chunkSize;
            }
            SINK_HANDLE.setOpaque(this, dummy);
        }

        private int addImpl(final long key1, final long key2, final long hash) {
            final byte control = control(hash);
            final int storedHash = storedHash(hash);
            int group = (int) hash & mask;
            for (;;) {
                final ByteVector vec = ByteVector.fromArray(BS, controlData, group);
                long matches = vec.eq(control).toLong();
                while (matches != 0) {
                    final int checkSlot = (group + Long.numberOfTrailingZeros(matches)) & mask;
                    final long packed = idAndHash(checkSlot);
                    if ((int) packed == storedHash) {
                        final int id = (int) (packed >>> 32);
                        if (equalKeys(keyOffset(id), key1, key2)) {
                            return -1 - id;
                        }
                    }
                    matches &= matches - 1;
                }
                final long empty = vec.eq(EMPTY).toLong();
                if (empty != 0) {
                    final int insertSlot = (group + Long.numberOfTrailingZeros(empty)) & mask;
                    final int id = size++;
                    final long packed = ((long) id << 32) | Integer.toUnsignedLong(storedHash);
                    final int idOffset = idOffset(insertSlot);
                    LONG_HANDLE.set(idPages[idOffset >> PAGE_SHIFT], idOffset & PAGE_MASK, packed);
                    insertAtSlot(insertSlot, control);
                    setKeys(keyOffset(id), key1, key2);
                    return id;
                }
                group = (group + BYTE_VECTOR_LANES) & mask;
            }
        }

        private void insertAtSlot(final int insertSlot, final byte control) {
            controlData[insertSlot] = control;
            if (insertSlot < BYTE_VECTOR_LANES) {
                controlData[insertSlot + capacity] = control;
            }
        }

        @Override
        protected Status status() {
            return new BigCoreStatus(growCount, capacity, size, nextGrowSize, insertProbes, keyPages.length, idPages.length);
        }

        @Override
        protected LongLongSwissHash.Itr iterator() {
            return new LongLongSwissHash.Itr() {
                @Override
                public boolean next() {
                    return ++keyId < size;
                }

                @Override
                public int id() {
                    return keyId;
                }

                @Override
                public long key1() {
                    return BigCore.this.key1(keyOffset(keyId));
                }

                @Override
                public long key2() {
                    return BigCore.this.key2(keyOffset(keyId));
                }
            };
        }

        private void grow(int minSize) {
            do {
                growTracking();
            } while (nextGrowSize < minSize);
            try {
                var newBigCore = new BigCore();
                rehash(newBigCore);
                bigCore = newBigCore;
            } finally {
                close();
            }
        }

        private void rehash(BigCore newBigCore) {
            if (size == 0) {
                return;
            }
            final int oldCapacity = controlData.length - BYTE_VECTOR_LANES;
            for (int slot = 0; slot < oldCapacity; slot++) {
                final byte ctrl = controlData[slot];
                if (ctrl == EMPTY) {
                    continue;
                }
                final long packed = idAndHash(slot);
                newBigCore.insert((int) packed, ctrl, (int) (packed >>> 32));
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
                    if (controlData[group + j] == EMPTY) {
                        final int insertSlot = (group + j) & mask;
                        insertAtSlot(insertSlot, control);
                        final long packed = ((long) id << 32) | Integer.toUnsignedLong(hash);
                        final int idOff = idOffset(insertSlot);
                        LONG_HANDLE.set(idPages[idOff >> PAGE_SHIFT], idOff & PAGE_MASK, packed);
                        return;
                    }
                }
                group = (group + BYTE_VECTOR_LANES) & mask;
                insertProbes++;
            }
        }

        private void appendKeys(long[] keys, int[] ids, int len) {
            assert size == 0 : size;
            int pageIndex = 0;
            byte[] keyPage = keyPages[0];
            int indexInPage = 0;
            for (int id = 0; id < len; id++) {
                long key1 = keys[id * 2];
                long key2 = keys[id * 2 + 1];
                long hash = hash(key1, key2);
                insert((int) hash, control(hash), id);
                if (indexInPage == PageCacheRecycler.PAGE_SIZE_IN_BYTES) {
                    keyPage = keyPages[++pageIndex];
                    indexInPage = 0;
                }
                LONG_HANDLE.set(keyPage, indexInPage, key1);
                LONG_HANDLE.set(keyPage, indexInPage + Long.BYTES, key2);
                indexInPage += KEY_SIZE;
            }
            for (int i = 0; i < len; i++) {
                ids[i] = i;
            }
            size = len;
        }

        /**
         * Returns {@code true} if every key was appended, allowing the caller to apply append-only optimizations.
         */
        boolean mergeKeys(long[] keys, int[] ids, int len) {
            Objects.checkFromIndexSize(0, len, keys.length / 2);
            Objects.checkFromIndexSize(0, len, ids.length);

            final int preSize = size;
            if (preSize == 0) {
                appendKeys(keys, ids, len);
                return true;
            }
            for (int i = 0; i < len; i++) {
                long k1 = keys[i * 2];
                long k2 = keys[i * 2 + 1];
                long hash = hash(k1, k2);
                int id = addImpl(k1, k2, hash);
                ids[i] = id >= 0 ? id : -1 - id;
            }
            return size == preSize + len;
        }

        private boolean equalKeys(final long keyOffset, final long key1, final long key2) {
            final byte[] page = keyPages[(int) (keyOffset >> PAGE_SHIFT)];
            final int indexInPage = (int) (keyOffset & PAGE_MASK);
            return (long) LONG_HANDLE.get(page, indexInPage) == key1 && (long) LONG_HANDLE.get(page, indexInPage + Long.BYTES) == key2;
        }

        private long key1(final long keyOffset) {
            final int keyPageOffset = Math.toIntExact(keyOffset >> PAGE_SHIFT);
            final int keyPageMask = Math.toIntExact(keyOffset & PAGE_MASK);
            return (long) LONG_HANDLE.get(keyPages[keyPageOffset], keyPageMask);
        }

        private long key2(final long keyOffset) {
            final int keyPageOffset = Math.toIntExact(keyOffset >> PAGE_SHIFT);
            final int keyPageMask = Math.toIntExact((keyOffset + Long.BYTES) & PAGE_MASK);
            return (long) LONG_HANDLE.get(keyPages[keyPageOffset], keyPageMask);
        }

        private void setKeys(final long keyOffset, final long key1, final long key2) {
            final int keyPageOffset = Math.toIntExact(keyOffset >> PAGE_SHIFT);
            final int keyPageMask = Math.toIntExact(keyOffset & PAGE_MASK);
            final byte[] page = keyPages[keyPageOffset];
            LONG_HANDLE.set(page, keyPageMask, key1);
            LONG_HANDLE.set(page, keyPageMask + Long.BYTES, key2);
        }

        private int idOffset(final int slot) {
            return slot * ID_AND_HASH;
        }

        private long idAndHash(final int slot) {
            final int idOffset = idOffset(slot);
            return (long) LONG_HANDLE.get(idPages[idOffset >> PAGE_SHIFT], idOffset & PAGE_MASK);
        }

        @Override
        public long ramBytesUsed() {
            return BASE_RAM_BYTES_USED + controlData.length + (long) idPages.length * PageCacheRecycler.PAGE_SIZE_IN_BYTES;
        }

        void clear() {
            Arrays.fill(controlData, EMPTY);
            insertProbes = 0;
        }
    }

    @Override
    public long getKey1(final long id) {
        final int actualId = Math.toIntExact(id);
        Objects.checkIndex(actualId, size());
        final long keyOffset = keyOffset(actualId);
        return smallCore != null ? smallCore.key1(Math.toIntExact(keyOffset)) : bigCore.key1(keyOffset);
    }

    @Override
    public long getKey2(final long id) {
        final int actualId = Math.toIntExact(id);
        Objects.checkIndex(actualId, size());
        final long keyOffset = keyOffset(actualId);
        return smallCore != null ? smallCore.key2(Math.toIntExact(keyOffset)) : bigCore.key2(keyOffset);
    }

    private long keyOffset(final int id) {
        return (long) id * KEY_SIZE;
    }

    private static long hash(long key1, long key2) {
        long h = key1 * 0x9E3779B97F4A7C15L ^ key2;
        h = (h ^ (h >>> 32)) * 0x4cd6944c5cc20b6dL;
        h = (h ^ (h >>> 29)) * 0xfc12c5b19d3259e9L;
        return h ^ (h >>> 32);
    }

    private int slot(final long hash) {
        return (int) hash & mask;
    }

    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + (smallCore != null ? smallCore.ramBytesUsed() : bigCore.ramBytesUsed());
    }

    static final class LongLongPartitionedHashKeys implements PartitionedHashKeys {
        final int[] partitionCounts;
        final long[][] partitionKeys;

        LongLongPartitionedHashKeys(CircuitBreaker breaker, int estimatePartitionCount) {
            estimatePartitionCount = ArrayUtil.oversize(estimatePartitionCount, Long.BYTES * 2);
            long usedBytes = (long) NUM_PARTITIONS * Integer.BYTES + (long) NUM_PARTITIONS * estimatePartitionCount * Long.BYTES * 2;
            breaker.addEstimateBytesAndMaybeBreak(usedBytes, "LongLongSwissHash#partition");
            final long[][] partitionKeys = new long[NUM_PARTITIONS][];
            for (int i = 0; i < NUM_PARTITIONS; i++) {
                partitionKeys[i] = new long[estimatePartitionCount * 2];
            }
            this.partitionKeys = partitionKeys;
            this.partitionCounts = new int[NUM_PARTITIONS];
        }

        void splitKeys(CircuitBreaker breaker, byte[][] keyPages, int idOffset, short[] positions, int[] fills) {
            assert NUM_PARTITIONS * PARTITION_WRITE_BATCH < Short.MAX_VALUE : "shifted ids of one batch must fit in the short value range";
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                final int c = fills[p];
                if (c == 0) {
                    continue;
                }
                int writeOffset = partitionCounts[p] * 2;
                ensureCapacity(breaker, p, writeOffset + c * 2);
                var sub = partitionKeys[p];
                final int base = p * PARTITION_WRITE_BATCH;
                for (int i = 0; i < c; i++) {
                    final int id = idOffset + (positions[base + i] & 0xFFFF);
                    final long keyOffset = (long) id * KEY_SIZE;
                    final int pageIndex = (int) (keyOffset >> PAGE_SHIFT);
                    final int indexInPage = (int) (keyOffset & PAGE_MASK);
                    byte[] keyPage = keyPages[pageIndex];
                    sub[writeOffset] = (long) LONG_HANDLE.get(keyPage, indexInPage);
                    sub[writeOffset + 1] = (long) LONG_HANDLE.get(keyPage, indexInPage + Long.BYTES);
                    writeOffset += 2;
                }
            }
        }

        private void ensureCapacity(CircuitBreaker breaker, int p, int minLength) {
            long[] sub = partitionKeys[p];
            final int currentLength = sub.length;
            if (currentLength >= minLength) {
                return;
            }
            final int newLength = ArrayUtil.oversize(minLength, Long.BYTES);
            final long deltaBytes = (long) newLength * Long.BYTES;
            breaker.addEstimateBytesAndMaybeBreak(deltaBytes, "LongLongSwissHash#partition");
            partitionKeys[p] = Arrays.copyOf(sub, newLength);
            breaker.addWithoutBreaking(-(long) currentLength * Long.BYTES);
        }

        @Override
        public void releasePartition(CircuitBreaker breaker, int partition) {
            long bytes = (long) partitionKeys[partition].length * Long.BYTES;
            partitionKeys[partition] = null;
            breaker.addWithoutBreaking(-bytes);
        }

        @Override
        public int keysInPartition(int partition) {
            return partitionCounts[partition];
        }

        @Override
        public void releaseAll(CircuitBreaker breaker) {
            long bytes = (long) NUM_PARTITIONS * Integer.BYTES;
            for (long[] sub : partitionKeys) {
                if (sub != null) {
                    bytes += (long) sub.length * Long.BYTES;
                }
            }
            breaker.addWithoutBreaking(-bytes);
        }
    }

    @Override
    public PartitionedHashKeys splitPartition(CircuitBreaker breaker, PartitionSplitter partitionSplitter) {
        final int[] batchPartitionCounts = new int[NUM_PARTITIONS];
        final short[] shiftedIds = new short[PARTITION_WRITE_BATCH * NUM_PARTITIONS];
        int batchStart = 0;
        int pageIndex = 0;
        byte[] keyPage = keyPages[0];
        int indexInPage = 0;
        var partitionedKeys = new LongLongPartitionedHashKeys(breaker, Math.max(Math.ceilDiv(size, NUM_PARTITIONS), 1));
        final int[] partitionOffsets = partitionedKeys.partitionCounts;
        boolean success = false;
        try {
            for (int id = 0; id < size; id++) {
                if (indexInPage >= PageCacheRecycler.PAGE_SIZE_IN_BYTES) {
                    keyPage = keyPages[++pageIndex];
                    indexInPage = 0;
                }
                final long key1 = (long) LONG_HANDLE.get(keyPage, indexInPage);
                final long key2 = (long) LONG_HANDLE.get(keyPage, indexInPage + Long.BYTES);
                indexInPage += KEY_SIZE;
                final long hash64 = hash(key1, key2);
                final int p = partition(hash64);
                if (batchPartitionCounts[p] == PARTITION_WRITE_BATCH) {
                    partitionedKeys.splitKeys(breaker, keyPages, batchStart, shiftedIds, batchPartitionCounts);
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
            partitionedKeys.splitKeys(breaker, keyPages, batchStart, shiftedIds, batchPartitionCounts);
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
        final LongLongPartitionedHashKeys subKeys = (LongLongPartitionedHashKeys) keys;
        assert subKeys.partitionKeys[partitionIndex] != null : "partition [" + partitionIndex + "] was already released";
        final int keysInPartition = subKeys.keysInPartition(partitionIndex);
        assert keysInPartition <= resultIds.length : keysInPartition + " > " + resultIds.length;
        final int needed = Math.addExact(size, keysInPartition);
        ensureCapacity(needed);
        if (smallCore != null) {
            return smallCore.mergeKeys(subKeys.partitionKeys[partitionIndex], resultIds, keysInPartition);
        } else {
            return bigCore.mergeKeys(subKeys.partitionKeys[partitionIndex], resultIds, keysInPartition);
        }
    }

    /**
     * Grows this table so it can hold at least {@code minSize} entries without future resizing
     */
    public void ensureCapacity(int minSize) {
        if (nextGrowSize < minSize) {
            if (smallCore != null) {
                smallCore.transitionToBigCore(minSize);
            } else {
                bigCore.grow(minSize);
            }
        }
    }

    /**
     * Removes all entries while retaining the allocated capacity, including the key pages, so the table can be refilled without resizing.
     */
    public void clear() {
        size = 0;
        growCount = 0;
        if (smallCore != null) {
            smallCore.clear();
        } else {
            bigCore.clear();
        }
    }
}
