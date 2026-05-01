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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** SwissHash for fixed-length byte keys. Keys never span page boundaries. */
public class FixedLengthSwissHash extends SwissHash implements BytesRefHashTable {

    static final VectorSpecies<Byte> BS = ByteVector.SPECIES_128;

    private static final int BYTE_VECTOR_LANES = BS.vectorByteSize();

    private static final int PAGE_SHIFT = 14;

    private static final int PAGE_MASK = PageCacheRecycler.PAGE_SIZE_IN_BYTES - 1;

    static final int INITIAL_CAPACITY = 1024;

    public static final int BATCH_SIZE = 128;
    public static final int MAX_KEY_SIZE = 64;

    static {
        if (PageCacheRecycler.PAGE_SIZE_IN_BYTES >> PAGE_SHIFT != 1) {
            throw new AssertionError("bad constants");
        }
        if (Integer.highestOneBit(INITIAL_CAPACITY) != INITIAL_CAPACITY) {
            throw new AssertionError("not a power of two");
        }
    }

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FixedLengthSwissHash.class);

    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());
    static final int DEFAULT_BULK_THRESHOLD = (int) ((1 << 17) * BigCore.FILL_FACTOR); // ~114k entries

    private SmallCore smallCore;
    private BigCore bigCore;
    private byte[][] keyPages;
    private long usedBytesByKeyPages = 0;
    private final int bulkThreshold;
    private final int keyLength;
    private final int keysPerPage;
    private final int maxKeyBytesInOnePage;
    private int nextKeyPageIndex;
    private int nextKeyPageOffset;
    private byte[] currentKeyPage;
    private final List<Releasable> toClose = new ArrayList<>();

    FixedLengthSwissHash(PageCacheRecycler recycler, CircuitBreaker breaker, int keyLength) {
        this(recycler, breaker, keyLength, DEFAULT_BULK_THRESHOLD);
    }

    FixedLengthSwissHash(PageCacheRecycler recycler, CircuitBreaker breaker, int keyLength, int bulkThreshold) {
        super(recycler, breaker, INITIAL_CAPACITY, LongSwissHash.SmallCore.FILL_FACTOR);
        this.keyLength = keyLength;
        this.keysPerPage = PageCacheRecycler.PAGE_SIZE_IN_BYTES / keyLength;
        this.maxKeyBytesInOnePage = keysPerPage * keyLength;
        this.bulkThreshold = bulkThreshold;
        boolean success = false;
        try {
            if (keyLength > MAX_KEY_SIZE) {
                throw new IllegalArgumentException(
                    "FixedLengthSwissHash supports keys with length up to " + MAX_KEY_SIZE + "; use BytesRefSwissHash for larger keys"
                );
            }
            smallCore = new SmallCore();
            int pagesNeeded = (nextGrowSize + keysPerPage - 1) / keysPerPage;
            keyPages = new byte[pagesNeeded + 1][];
            keyPages[0] = currentKeyPage = grabKeyPage();
            nextKeyPageIndex = 0;
            nextKeyPageOffset = 0;
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    private byte[] grabKeyPage() {
        breaker.addEstimateBytesAndMaybeBreak(PageCacheRecycler.PAGE_SIZE_IN_BYTES, "FixedLengthSwissHash-keyPage");
        usedBytesByKeyPages += PageCacheRecycler.PAGE_SIZE_IN_BYTES;
        Recycler.V<byte[]> page = recycler.bytePage(false);
        toClose.add(page);
        return page.v();
    }

    /**
     * Finds an {@code id} by a {@link BytesRef} key.
     */
    @Override
    public long find(final BytesRef key) {
        final long hash = hash(key.bytes, key.offset, keyLength);
        if (smallCore != null) {
            return smallCore.find(key.bytes, key.offset, hash);
        } else {
            return bigCore.find(key.bytes, key.offset, hash, control(hash));
        }
    }

    /**
     * Add a {@code key}, returning its {@code id}s. If it was already present
     * it's previous assigned {@code id} will be returned. If it wasn't present
     * it'll be assigned a new {@code id}.
     */
    @Override
    public long add(final BytesRef key) {
        final long hash = hash(key.bytes, key.offset, keyLength);
        if (smallCore != null) {
            if (size < nextGrowSize) {
                return smallCore.add(key.bytes, key.offset, hash);
            }
            smallCore.transitionToBigCore();
        }
        return bigCore.add(key.bytes, key.offset, hash);
    }

    public long numKeys() {
        return size;
    }

    @Override
    public boolean supportBulkAdd() {
        return bigCore != null && size > bulkThreshold;
    }

    @Override
    public void bulkAdd(byte[] keysBytes, int startOffset, int[] ids, int idsOffset, int numKeys) {
        assert bigCore != null : "must call supportBulkAdd before";
        final long needed = (long) size + numKeys;
        while (nextGrowSize <= needed) {
            bigCore.grow();
        }
        bigCore.batchAdd(keysBytes, startOffset, ids, idsOffset, numKeys);
    }

    @Override
    public Status status() {
        return smallCore != null ? smallCore.status() : bigCore.status();
    }

    @Override
    public SwissHash.Itr iterator() {
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
     * replaced by a {@link FixedLengthSwissHash.BigCore}.
     *
     * <p> This uses one page from the {@link PageCacheRecycler} for the
     * {@code ids}.
     */
    final class SmallCore extends Core implements Accountable {
        static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(SmallCore.class);

        private final int[] ids;

        private SmallCore() {
            ids = new int[capacity];
            Arrays.fill(ids, -1);
        }

        int find(final byte[] key, final int keyOffset, final long hash) {
            int slot = slot(hash);
            for (;;) {
                int id = ids[slot];
                if (id < 0) {
                    return -1;
                }
                if (equalKeys(id, key, keyOffset)) {
                    return id;
                }
                slot = slot(slot + 1);
            }
        }

        int add(final byte[] key, final int keyOffset, final long hash) {
            int slot = slot(hash);
            for (;;) {
                final int id = ids[slot];
                if (id < 0) {
                    int newId = size;
                    appendKeys(key, keyOffset);
                    ids[slot] = newId;
                    size++;
                    return newId;
                }
                if (equalKeys(id, key, keyOffset)) {
                    return -1 - id;
                }
                slot = slot(slot + 1);
            }
        }

        void transitionToBigCore() {
            growTracking();
            try {
                bigCore = new BigCore();
                for (int id = 0; id < size; id++) {
                    final var page = keyPages[Math.toIntExact(id / keysPerPage)];
                    final long hash = hash(page, (id % keysPerPage) * keyLength, keyLength);
                    bigCore.insert(storedHash(hash), control(hash), id);
                }
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
        protected SwissHash.Itr iterator() {
            return new SwissHash.Itr() {
                @Override
                public boolean next() {
                    return ++keyId < size;
                }

                @Override
                public int id() {
                    return keyId;
                }
            };
        }

        @Override
        public long ramBytesUsed() {
            return BASE_RAM_BYTES_USED + (long) ids.length * Integer.BYTES;
        }
    }

    final class BigCore extends Core implements Accountable {
        static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(BigCore.class);

        static final float FILL_FACTOR = 0.875F;

        private static final byte EMPTY = (byte) 0x80; // empty slot
        private static final int ID_AND_HASH = Integer.BYTES + Integer.BYTES;

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
            breaker.addEstimateBytesAndMaybeBreak(controlLength, "FixedLengthSwissHash-bigCore");
            toClose.add(() -> breaker.addWithoutBreaking(-controlLength));
            controlData = new byte[controlLength];
            Arrays.fill(controlData, EMPTY);

            boolean success = false;
            try {
                final int keyPagesNeeded = (nextGrowSize + keysPerPage - 1) / keysPerPage;
                keyPages = Arrays.copyOf(keyPages, keyPagesNeeded + 1);
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

        private int find(final byte[] key, final int keyOffset, final long hash, final byte control) {
            final int sh = storedHash(hash);
            int group = (int) hash & mask;
            for (;;) {
                final ByteVector vec = ByteVector.fromArray(BS, controlData, group);
                long matches = vec.eq(control).toLong();
                while (matches != 0) {
                    final int checkSlot = (group + Long.numberOfTrailingZeros(matches)) & mask;
                    final long packed = idAndHash(checkSlot);
                    if ((int) packed == sh) {
                        final int id = (int) (packed >>> 32);
                        if (equalKeys(id, key, keyOffset)) {
                            return id;
                        }
                    }
                    matches &= matches - 1;
                }
                long empty = vec.eq(EMPTY).toLong();
                if (empty != 0) {
                    return -1;
                }
                group = (group + BYTE_VECTOR_LANES) & mask;
            }
        }

        private int add(final byte[] key, final int keyOff, final long hash) {
            maybeGrow();
            return bigCore.addImpl(key, keyOff, hash);
        }

        private final long[] batchHashes = new long[BATCH_SIZE];
        private final byte[][] batchIdPageRefs = new byte[BATCH_SIZE][];

        private void batchAdd(byte[] keyArray, int startOffset, int[] ids, int idsOffset, int numKeys) {
            int offset = 0;
            long dummy = 0;
            while (offset < numKeys) {
                final int chunkSize = Math.min(numKeys - offset, BATCH_SIZE);
                for (int i = 0; i < chunkSize; i++) {
                    final int keyOffset = startOffset + (offset + i) * keyLength;
                    final long hash = hash(keyArray, keyOffset, keyLength);
                    batchHashes[i] = hash;
                    int idOff = idOffset((int) hash & mask);
                    batchIdPageRefs[i] = idPages[idOff >> PAGE_SHIFT];
                }
                for (int i = 0; i < chunkSize; i++) {
                    int group = ((int) batchHashes[i]) & mask;
                    dummy ^= controlData[group];
                    dummy ^= batchIdPageRefs[i][idOffset(group) & PAGE_MASK];
                }
                for (int r = 0; r < chunkSize; r++) {
                    final int keyOffset = startOffset + (offset + r) * keyLength;
                    final int id = addImpl(keyArray, keyOffset, batchHashes[r]);
                    ids[idsOffset + offset + r] = id >= 0 ? id : -1 - id;
                }
                offset += chunkSize;
            }
            SINK_HANDLE.setOpaque(this, dummy);
        }

        private int addImpl(final byte[] key, final int keyOffset, final long hash) {
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
                        if (equalKeys(id, key, keyOffset)) {
                            return -1 - id;
                        }
                    }
                    matches &= matches - 1;
                }
                final long empty = vec.eq(EMPTY).toLong();
                if (empty != 0) {
                    final int insertSlot = (group + Long.numberOfTrailingZeros(empty)) & mask;
                    final int id = size++;
                    appendKeys(key, keyOffset);
                    final long packed = ((long) id << 32) | Integer.toUnsignedLong(storedHash);
                    final int idOffset = idOffset(insertSlot);
                    LONG_HANDLE.set(idPages[idOffset >> PAGE_SHIFT], idOffset & PAGE_MASK, packed);
                    insertAtSlot(insertSlot, control);
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
        protected SwissHash.Itr iterator() {
            return new SwissHash.Itr() {
                @Override
                public boolean next() {
                    return ++keyId < size;
                }

                @Override
                public int id() {
                    return keyId;
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
    }

    private boolean equalKeys(final int id, final byte[] key, final int keyOffset) {
        final int pageIndex = Math.toIntExact(id / keysPerPage);
        final byte[] page = keyPages[pageIndex];
        final int pageOffset = (id - pageIndex * keysPerPage) * keyLength;
        return Arrays.mismatch(page, pageOffset, pageOffset + keyLength, key, keyOffset, keyOffset + keyLength) == -1;
    }

    private void appendKeys(final byte[] key, final int keyOffset) {
        System.arraycopy(key, keyOffset, currentKeyPage, nextKeyPageOffset, keyLength);
        nextKeyPageOffset += keyLength;
        if (nextKeyPageOffset == maxKeyBytesInOnePage) {
            nextKeyPageIndex++;
            nextKeyPageOffset = 0;
            keyPages[nextKeyPageIndex] = currentKeyPage = grabKeyPage();
        }
    }

    @Override
    public BytesRef get(long id, BytesRef dest) {
        final int pageIndex = Math.toIntExact(id / keysPerPage);
        dest.bytes = keyPages[pageIndex];
        dest.offset = ((int) (id) - pageIndex * keysPerPage) * keyLength;
        dest.length = keyLength;
        return dest;
    }

    private final BytesRef cursorScratch = new BytesRef(new byte[MAX_KEY_SIZE]);

    @Override
    public long add(PagedBytesCursor key) {
        BytesRef ref = key.readBytes(keyLength, cursorScratch);
        return add(ref);
    }

    @Override
    public BytesRefArray getOptionalBackingBytesRefs() {
        return null;
    }

    private static long hash(byte[] bytes, int offset, int length) {
        long h = 0x9E3779B97F4A7C15L;
        int pos = offset;
        int remaining = length;
        while (remaining >= 8) {
            h = (h ^ (long) LONG_HANDLE.get(bytes, pos)) * 0x4cf5ad432745937fL;
            pos += 8;
            remaining -= 8;
        }
        if (remaining >= 4) {
            h = (h ^ Integer.toUnsignedLong((int) INT_HANDLE.get(bytes, pos))) * 0x4cf5ad432745937fL;
            pos += 4;
            remaining -= 4;
        }
        for (int i = 0; i < remaining; i++) {
            h ^= Byte.toUnsignedLong(bytes[pos + i]) << (i * 8);
        }
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
}
