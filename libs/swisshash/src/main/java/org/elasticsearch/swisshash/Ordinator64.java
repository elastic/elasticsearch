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

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.simdvec.ESVectorUtil;
import org.elasticsearch.simdvec.VectorComparisonUtils;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
 * This allows us to run a much higher load factor (85%) without any performance
 * penalty so the extra byte feels super worth it.
 *
 * <p> When a "big core" fills it's table to the fill factor, we build a new
 * "big core" and read all values in the old "big core" into the new one.
 *
 * <p> This class does not store the keys in the hash table slots. Instead, it
 * uses a {@link #keyPages} to store the actual values, and the hash table
 * slots store the {@code id} which indexes into the {@link #keyPages}.
 */
public final class Ordinator64 extends Ordinator implements Releasable {
    private static final VectorComparisonUtils VECTOR_UTILS = ESVectorUtil.getVectorComparisonUtils();

    private static final int BYTE_VECTOR_LANES = VECTOR_UTILS.byteVectorLanes();

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

    public Ordinator64(PageCacheRecycler recycler, CircuitBreaker breaker) {
        super(recycler, breaker, INITIAL_CAPACITY, Ordinator64.SmallCore.FILL_FACTOR);
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
        breaker.addEstimateBytesAndMaybeBreak(PageCacheRecycler.PAGE_SIZE_IN_BYTES, "ordinator64-keyPage");
        toClose.add(() -> breaker.addWithoutBreaking(-PageCacheRecycler.PAGE_SIZE_IN_BYTES));
        Recycler.V<byte[]> page = recycler.bytePage(false);
        toClose.add(page);
        return page.v();
    }

    /**
     * Finds an {@code id} by a {@code key}.
     */
    public int find(final long key) {
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
    public void add(long[] keys, int[] ids, int length) {
        int i = 0;
        for (; i < length; i++) {
            if (bigCore != null) {
                for (; i < length; i++) {
                    long k = keys[i];
                    ids[i] = bigCore.add(k, hash(k));
                }
                return;
            }

            ids[i] = add(keys[i]);
        }
    }

    /**
     * Add a {@code key}, returning its {@code id}s. If it was already present
     * it's previous assigned {@code id} will be returned. If it wasn't present
     * it'll be assigned a new {@code id}.
     */
    public int add(final long key) {
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

    public abstract class Itr extends Ordinator.Itr {
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
     * Open addressed hash table the probes by triangle numbers. Empty
     * {@code id}s are encoded as {@code -1}. This hash table can't
     * grow, and is instead replaced by a {@link BigCore}.
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
            int slotIncrement = 0; // increment for probing by triangle numbers
            int slot = slot(hash);
            while (true) {
                int id = id(slot);
                if (id < 0) {
                    return -1; // empty
                }
                if (key(id) == key) {
                    return id;
                }
                slotIncrement++;
                slot = slot(slot + slotIncrement);
            }
        }

        int add(final long key, final int hash) {
            int slotIncrement = 0; // increment for probing by triangle numbers
            int slot = slot(hash);
            while (true) {
                final int idOffset = idOffset(slot);
                final int currentId = (int) INT_HANDLE.get(idPage, idOffset);
                if (currentId >= 0) {
                    final long currentKey = key(currentId);
                    if (currentKey == key) {
                        return currentId;
                    }
                    slotIncrement++;
                    slot = slot(slot + slotIncrement);
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
                public long key() {
                    return SmallCore.this.key(id());
                }
            };
        }

        private void rehash(final int oldCapacity) {
            for (int slot = 0; slot < oldCapacity; slot++) {
                final int id = id(slot);
                if (id < 0) {
                    continue;
                }
                final long key = key(id);
                final int hash = hash(key);
                bigCore.insert(hash, control(hash), id);
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
     * A SwissTable inspired hashtable. This differs from the normal SwissTable
     * in because it's adapted to Elasticsearch's {@link PageCacheRecycler}.
     * The keys and ids are stored many {@link PageCacheRecycler#PAGE_SIZE_IN_BYTES}
     * arrays, with the keys separated from the values. This is mostly so that we
     * can be sure the array and offset into the array can be calculated by right
     * shifts.
     */
    final class BigCore extends Core {
        static final float FILL_FACTOR = 0.85F;

        private static final byte CONTROL_EMPTY = (byte) 0b1111_1111;

        /**
         * The "control" bytes from the SwissTable algorithm. This will contain
         * {@link #CONTROL_EMPTY} for empty entries and {@code 0b0aaa_aaaa} for
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
            breaker.addEstimateBytesAndMaybeBreak(controlLength, "ordinator64-bigCore");
            toClose.add(() -> breaker.addWithoutBreaking(-controlLength));
            controlData = new byte[controlLength];
            Arrays.fill(controlData, (byte) 0xFF);

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
         * <p> We probe via triangle numbers, adding 1, then 2, then 3, then 4, etc.
         * That'll help protect us from chunky hashes. And it's simple math. And it'll
         * hit all the buckets (<a href="https://fgiesen.wordpress.com/2015/02/22/triangular-numbers-mod-2n/">proof</a>).
         * The probe loop doesn't stop if it never finds an EMPTY flag. But it'll always
         * find one because we keep a load factor lower than 100%.
         */
        private int find(final long key, final int hash, final byte control) {
            int slotIncrement = 0; // increment for probing by triangle numbers
            int slot = slot(hash);
            while (true) {
                long candidateMatches = controlMatches(slot, control);
                int first;
                while ((first = VectorComparisonUtils.firstSet(candidateMatches)) != -1) {
                    final int checkSlot = slot(slot + first);
                    final int id = id(checkSlot);
                    if (key(id) == key) {
                        return id;
                    }
                    // Clear the first set bit and try again
                    candidateMatches &= ~(1L << first);
                }
                if (controlMatches(slot, CONTROL_EMPTY) != 0) {
                    return -1;
                }
                slotIncrement += BYTE_VECTOR_LANES;
                slot = slot(slot + slotIncrement);
            }
        }

        private int add(final long key, final int hash) {
            final byte control = control(hash);
            final int found = find(key, hash, control);
            if (found >= 0) {
                return found;
            }

            if (size >= nextGrowSize) {
                assert size == nextGrowSize;
                grow();
            }

            final int id = size;
            setKey(id, key);
            bigCore.insert(hash, control, id);
            size++;
            return id;
        }

        /**
         * Inserts the key into the first empty slot that allows it. Used by {@link #add}
         * after we verify that the key isn't in the index. And used by {@link #rehash}
         * because we know all keys are unique.
         */
        private void insert(final int hash, final byte control, final int id) {
            int slotIncrement = 0; // increment for probing by triangle numbers
            int slot = slot(hash);
            while (true) {
                long empty = controlMatches(slot, CONTROL_EMPTY);
                if (VectorComparisonUtils.anyTrue(empty)) {
                    final int insertSlot = slot(slot + VectorComparisonUtils.firstSet(empty));
                    final int idOffset = idOffset(insertSlot);
                    INT_HANDLE.set(idPages[idOffset >> PAGE_SHIFT], idOffset & PAGE_MASK, id);
                    controlData[insertSlot] = control;
                    // mirror only if slot is within the first group size, to handle wraparound loads
                    if (insertSlot < BYTE_VECTOR_LANES) {
                        controlData[insertSlot + capacity] = control;
                    }
                    return;
                }
                slotIncrement += BYTE_VECTOR_LANES;
                slot = slot(slot + slotIncrement);
                insertProbes++;
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
                public long key() {
                    return BigCore.this.key(id());
                }
            };
        }

        private void grow() {
            int oldCapacity = growTracking();
            try {
                var newBigCore = new BigCore();
                rehash(oldCapacity, newBigCore);
                bigCore = newBigCore;
            } finally {
                close();
            }
        }

        private void rehash(final int oldCapacity, BigCore newBigCore) {
            int slot = 0;
            while (slot < oldCapacity) {
                long empty = controlMatches(slot, CONTROL_EMPTY);
                for (int i = 0; i < BYTE_VECTOR_LANES && slot + i < oldCapacity; i++) {
                    if ((empty & (1L << i)) != 0) {
                        continue;
                    }
                    final int actualSlot = slot + i;
                    final int id = id(actualSlot);
                    final int hash = hash(key(id));
                    newBigCore.insert(hash, control(hash), id);
                }
                slot += BYTE_VECTOR_LANES;
            }
        }

        /**
         * Checks the control byte at {@code slot} and the next few bytes ahead
         * of {@code slot} for the control bits. The extra probed bytes is as
         * many as will fit in your widest simd instruction. So, 32 or 64 will
         * be common.
         */
        private long controlMatches(final int slot, final byte control) {
            return VECTOR_UTILS.equalMask(controlData, slot, control);
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
     * Returns the key at <code>0 &lt;= index &lt;= capacity()</code>. The result is undefined if the slot is unused.
     */
    public long get(final int index) {
        if (this.bigCore == null) {
            // return this.smallCore.key(id(slot));
            return this.smallCore.key(index);
        }
        return bigCore.key(index);  // TODO: get id from slot???
    }

    public int id(final int index) {
        if (this.bigCore == null) {
            return this.smallCore.id(index);
        }
        return bigCore.id(index);
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
