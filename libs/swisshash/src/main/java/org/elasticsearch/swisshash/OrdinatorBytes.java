/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.swisshash;

import com.carrotsearch.hppc.BitMixer;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.simdvec.VectorByteUtils;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * Assigns {@code int} ids to {@code BytesRef}s, vending the ids in order they are added.
 * <p>
 *     At it's core there are two hash table implementations, a "small core" and
 *     a "big core". The "small core" is a simple
 *     <a href="https://en.wikipedia.org/wiki/Open_addressing">open addressed</a>
 *     hash table with a fixed 60% load factor and a table of 2048. It quite quick
 *     because it has a fixed size and never grows.
 * </p>
 * <p>
 *     When the "small core" has more entries than it's load factor the "small core"
 *     is replaced with a "big core". The "big core" functions quite similarly to
 *     a <a href="https://faultlore.com/blah/hashbrown-tldr/">Swisstable</a>, Google's
 *     fancy SIMD hash table. In this table there's a contiguous array of "control"
 *     bytes that are either {@code 0b1111_1111} for empty entries or
 *     {@code 0b0aaa_aaaa} for populated entries, where {@code aaa_aaaa} are the top
 *     7 bytes of the hash. To find an entry by key you hash it, grab the top 7 bytes
 *     or it, and perform a SIMD read of the control array starting at the expected
 *     slot. We use the widest SIMD instruction the CPU supports, meaning 64 or 32
 *     bytes. If any of those match we check the actual key. So instead of scanning
 *     one slot at a time "small core", we effectively scan a whole bunch at once.
 *     This allows us to run a much higher load factor (85%) without any performance
 *     penalty so the extra byte feels super worth it.
 * </p>
 * <p>
 *     When a "big core" fills it's table to the fill factor, we build a new "big core"
 *     and read all values in the old "big core" into the new one.
 * </p>
 * <p>
 *     Unlike {@link Ordinator64}, this class does not store the keys in the hash table slots.
 *     Instead, it uses a {@link BytesRefArray} to store the actual bytes, and the hash table
 *     slots store the {@code id} which indexes into the {@link BytesRefArray}.
 * </p>
 */
public class OrdinatorBytes extends Ordinator implements Releasable {
    private static final VectorByteUtils VECTOR_UTILS = ESVectorUtil.getVectorByteUtils();

    private static final int PAGE_SHIFT = 14;

    private static final int PAGE_MASK = PageCacheRecycler.PAGE_SIZE_IN_BYTES - 1;

    private static final int ID_SIZE = Integer.BYTES;

    // We use a smaller initial capacity than Ordinator64 because we don't store keys in pages,
    // but we want to be consistent with the page-based sizing logic.
    // PAGE_SIZE / ID_SIZE = 16384 / 4 = 4096.
    static final int INITIAL_CAPACITY = PageCacheRecycler.PAGE_SIZE_IN_BYTES / ID_SIZE;

    static {
        if (PageCacheRecycler.PAGE_SIZE_IN_BYTES >> PAGE_SHIFT != 1) {
            throw new AssertionError("bad constants");
        }
        if (Integer.highestOneBit(ID_SIZE) != ID_SIZE) {
            throw new AssertionError("not a power of two");
        }
        if (Integer.highestOneBit(INITIAL_CAPACITY) != INITIAL_CAPACITY) {
            throw new AssertionError("not a power of two");
        }
    }

    private static final VarHandle intHandle = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());

    private final BytesRefArray bytesRefs;
    private final boolean ownsBytesRefs;
    private final BytesRef scratch = new BytesRef();

    private SmallCore smallCore;
    private BigCore bigCore;

    /**
     * Create a new {@link OrdinatorBytes} that manages its own {@link BytesRefArray}.
     */
    public OrdinatorBytes(PageCacheRecycler recycler, CircuitBreaker breaker, BigArrays bigArrays) {
        this(recycler, breaker, new BytesRefArray(PageCacheRecycler.PAGE_SIZE_IN_BYTES, bigArrays), true);
    }

    /**
     * Create a new {@link OrdinatorBytes} that uses the provided {@link BytesRefArray}.
     * This allows multiple {@link OrdinatorBytes} to share the same key storage and ID space.
     */
    public OrdinatorBytes(PageCacheRecycler recycler, CircuitBreaker breaker, BytesRefArray bytesRefs) {
        this(recycler, breaker, bytesRefs, false);
    }

    private OrdinatorBytes(PageCacheRecycler recycler, CircuitBreaker breaker, BytesRefArray bytesRefs, boolean ownsBytesRefs) {
        // We pass a dummy IdSpace because we rely on BytesRefArray for ID generation
        super(recycler, breaker, new IdSpace(), INITIAL_CAPACITY, SmallCore.FILL_FACTOR);
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
     * Find an {@code id} by a {@code key}.
     */
    public int find(BytesRef key) {
        int hash = hash(key);
        if (smallCore != null) {
            return smallCore.find(key, hash);
        } else {
            return bigCore.find(key, hash, control(hash));
        }
    }

    /**
     * Add a {@code key}, returning its {@code id}. If it was already present
     * it's previous assigned {@code id} will be returned. If it wasn't present
     * it'll be assigned a new {@code id}.
     */
    public int add(BytesRef key) {
        int hash = hash(key);
        if (smallCore != null) {
            if (size < nextGrowSize) {
                return smallCore.add(key, hash);
            }
            smallCore.transitionToBigCore();
        }
        return bigCore.add(key, hash);
    }

    public BytesRefArray getBytesRefArray() {
        return bytesRefs;
    }

    @Override
    public Status status() {
        return smallCore != null ? smallCore.status() : bigCore.status();
    }

    public abstract class Itr extends Ordinator.Itr {
        /**
         * The key the iterator is current pointing to.
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
    private byte control(int hash) {
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
        // TODO what about MAX_INT?
        int oldCapacity = capacity;
        capacity <<= 1;
        mask = capacity - 1;
        nextGrowSize = (int) (capacity * BigCore.FILL_FACTOR);
        return oldCapacity;
    }

    /**
     * Open addressed hash table the probes by triangle numbers. Empty
     * {@code id}s are encoded as {@code -1}. This hash table can't
     * grow, and is instead replaced by a {@link BigCore}.
     * <p>
     *     This uses one page from the {@link PageCacheRecycler} for the {@code ids}.
     * </p>
     */
    class SmallCore extends Core {
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

        int find(BytesRef key, int hash) {
            int slotIncrement = 0;
            int slot = slot(hash);
            while (true) {
                int id = id(slot);
                if (id < 0) {
                    // Empty
                    return -1;
                }
                if (matches(key, id)) {
                    return id;
                }

                slotIncrement++;
                slot = slot(slot + slotIncrement);
            }
        }

        int add(BytesRef key, int hash) {
            int slotIncrement = 0;
            int slot = slot(hash);
            while (true) {
                int idOffset = idOffset(slot);
                int slotId = (int) intHandle.get(idPage, idOffset);
                if (slotId >= 0) {
                    if (matches(key, slotId)) {
                        return slotId;
                    }
                } else {
                    // We don't use idSpace.next() because BytesRefArray manages IDs
                    int id = (int) bytesRefs.size();
                    bytesRefs.append(key);
                    size++;
                    intHandle.set(idPage, idOffset, id);
                    return id;
                }

                slotIncrement += VECTOR_UTILS.vectorLength();
                slot = slot(slot + slotIncrement);
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
                    do {
                        slot++;
                    } while (slot < capacity && SmallCore.this.id(slot) < 0);
                    return slot < capacity;
                }

                @Override
                public int id() {
                    return SmallCore.this.id(slot);
                }

                @Override
                public BytesRef key(BytesRef dest) {
                    return bytesRefs.get(SmallCore.this.id(slot), dest);
                }
            };
        }

        private void rehash(int oldCapacity) {
            for (int slot = 0; slot < oldCapacity; slot++) {
                int id = (int) intHandle.get(idPage, idOffset(slot));
                if (id < 0) {
                    continue;
                }
                // Retrieve key from storage to rehash
                bytesRefs.get(id, scratch);
                int hash = hash(scratch);
                bigCore.insert(hash, control(hash), id);
            }
        }

        private int id(int slot) {
            return (int) intHandle.get(idPage, idOffset(slot));
        }
    }

    /**
     * A Swisstable inspired hashtable. This differs from the normal swisstable
     * in because it's adapted to Elasticsearch's {@link PageCacheRecycler}.
     * The ids are stored many {@link PageCacheRecycler#PAGE_SIZE_IN_BYTES}
     * arrays.
     */
    class BigCore extends Core {
        static final float FILL_FACTOR = 0.85F;

        private static final byte CONTROL_EMPTY = (byte) 0b1111_1111;

        /**
         * The "control" bytes from the Swisstable algorithm.
         */
        private final byte[] controlData;

        /**
         * Pages of {@code ids}, vended by the {@link PageCacheRecycler}. Ids
         * are {@code int}s so it's very quick to select the appropriate page
         * for each slot.
         */
        private final byte[][] idPages;

        private int insertProbes;

        BigCore() {
            int controlLength = capacity + VECTOR_UTILS.vectorLength();
            breaker.addEstimateBytesAndMaybeBreak(controlLength, "ordinator");
            toClose.add(() -> breaker.addWithoutBreaking(-controlLength));
            controlData = new byte[controlLength];
            Arrays.fill(controlData, (byte) 0xFF);

            boolean success = false;
            try {
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

        private int find(BytesRef key, int hash, byte control) {
            int slotIncrement = 0;
            int slot = slot(hash);
            while (true) {
                long candidateMatches = controlMatches(slot, control);

                int first;
                while ((first = VectorByteUtils.firstSet(candidateMatches)) != -1) {
                    int checkSlot = slot(slot + first);
                    int id = id(checkSlot);
                    if (matches(key, id)) {
                        return id;
                    }
                    // Clear the first set bit and try again
                    candidateMatches &= ~(1L << first);
                }

                if (VectorByteUtils.anyTrue(controlMatches(slot, CONTROL_EMPTY))) {
                    return -1;
                }

                slotIncrement += VECTOR_UTILS.vectorLength();
                slot = slot(slot + slotIncrement);
            }
        }

        int add(BytesRef key, int hash) {
            byte control = control(hash);
            int found = find(key, hash, control);
            if (found >= 0) {
                return found;
            }

            if (size >= nextGrowSize) {
                assert size == nextGrowSize;
                grow();
            }

            int id = (int) bytesRefs.size();
            bytesRefs.append(key);
            size++;
            bigCore.insert(hash, control, id);
            return id;
        }

        /**
         * Insert the key into the first empty slot that allows it. Used by {@link #add}
         * after we verify that the key isn't in the index. And used by {@link #rehash}
         * because we know all keys are unique.
         */
        void insert(int hash, byte control, int id) {
            int slotIncrement = 0;
            int slot = slot(hash);
            while (true) {
                long empty = controlMatches(slot, CONTROL_EMPTY);
                if (VectorByteUtils.anyTrue(empty)) {
                    slot = slot(slot + VectorByteUtils.firstSet(empty));
                    int idOffset = idOffset(slot);

                    intHandle.set(idPages[idOffset >> PAGE_SHIFT], idOffset & PAGE_MASK, id);
                    controlData[slot] = control;
                    /*
                     * Mirror the first VECTOR_UTILS.vectorLength() bytes to the end of the array. All
                     * other positions are just written twice.
                     */
                    controlData[((slot - VECTOR_UTILS.vectorLength()) & mask) + VECTOR_UTILS.vectorLength()] = control;
                    return;
                }

                slotIncrement += VECTOR_UTILS.vectorLength();
                slot = slot(slot + slotIncrement);
                insertProbes++;
            }
        }

        @Override
        protected Status status() {
            return new BigCoreStatus(growCount, capacity, size, nextGrowSize, insertProbes, 0, idPages.length);
        }

        @Override
        protected Itr iterator() {
            return new Itr() {
                @Override
                public boolean next() {
                    do {
                        slot++;
                    } while (slot < capacity && controlData[slot] == CONTROL_EMPTY);
                    return slot < capacity;
                }

                @Override
                public int id() {
                    return BigCore.this.id(slot);
                }

                @Override
                public BytesRef key(BytesRef dest) {
                    return bytesRefs.get(BigCore.this.id(slot), dest);
                }
            };
        }

        private void grow() {
            int oldCapacity = growTracking();
            try {
                bigCore = new BigCore();
                rehash(oldCapacity);
            } finally {
                close();
            }
        }

        private void rehash(int oldCapacity) {
            int slot = 0;
            while (slot < oldCapacity) {
                long empty = controlMatches(slot, CONTROL_EMPTY);
                // TODO iterate like in find - it's faster.
                for (int i = 0; i < VECTOR_UTILS.vectorLength() && slot + i < oldCapacity; i++) {
                    if ((empty & (1L << i)) != 0L) {
                        slot++;
                        continue;
                    }
                    int id = id(slot);
                    // Retrieve key to rehash
                    bytesRefs.get(id, scratch);
                    int hash = hash(scratch);
                    bigCore.insert(hash, control(hash), id);
                    slot++;
                }
            }
        }

        /**
         * Checks the control byte at {@code slot} and the next few bytes ahead
         * of {@code slot} for the control bits. The extra probed bytes is as
         * many as will fit in your widest simd instruction. So, 32 or 64 will
         * be common.
         */
        private long controlMatches(int slot, byte control) {
            return VECTOR_UTILS.equalMask(controlData, slot, control);
        }

        private int id(int slot) {
            int idOffset = idOffset(slot);
            return (int) intHandle.get(idPages[idOffset >> PAGE_SHIFT], idOffset & PAGE_MASK);
        }
    }

    /**
     * Fills the scratch BytesRef with the key at the specified slot.
     * This is compliant with the pattern used in Ordinator64, though here it involves
     * an indirect lookup via the ID.
     */
    public void key(int slot, BytesRef dest) {
        int id;
        if (this.bigCore == null) {
            id = this.smallCore.id(slot);
        } else {
            id = bigCore.id(slot);
        }
        if (id >= 0) {
            bytesRefs.get(id, dest);
        }
    }

    int id(int slot) {
        if (this.bigCore == null) {
            return this.smallCore.id(slot);
        }
        return bigCore.id(slot);
    }

    int idOffset(int slot) {
        return slot * ID_SIZE;
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
}