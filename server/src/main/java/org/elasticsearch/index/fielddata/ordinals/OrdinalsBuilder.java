/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PagedGrowableWriter;

import java.io.Closeable;
import java.util.Arrays;

/**
 * Simple class to build document ID &lt;-&gt; ordinal mapping. Note: Ordinals are
 * {@code 1} based monotonically increasing positive integers. {@code 0}
 * donates the missing value in this context.
 */
public final class OrdinalsBuilder implements Closeable {

    /**
     * Default acceptable overhead ratio. {@link OrdinalsBuilder} memory usage is mostly transient so it is likely a better trade-off to
     * trade memory for speed in order to resize less often.
     */
    public static final float DEFAULT_ACCEPTABLE_OVERHEAD_RATIO = PackedInts.FAST;

    /**
     * The following structure is used to store ordinals. The idea is to store ords on levels of increasing sizes. Level 0 stores
     * 1 value and 1 pointer to level 1. Level 1 stores 2 values and 1 pointer to level 2, ..., Level n stores 2**n values and
     * 1 pointer to level n+1. If at some point an ordinal or a pointer has 0 as a value, this means that there are no remaining
     * values. On the first level, ordinals.get(docId) is the first ordinal for docId or 0 if the document has no ordinals. On
     * subsequent levels, the first 2^level slots are reserved and all have 0 as a value.
     * <pre>
     * Example for an index of 3 docs (O=ordinal, P = pointer)
     * Level 0:
     *   ordinals           [1] [4] [2]
     *   nextLevelSlices    2  0  1
     * Level 1:
     *   ordinals           [0  0] [2  0] [3  4]
     *   nextLevelSlices    0  0  1
     * Level 2:
     *   ordinals           [0  0  0  0] [5  0  0  0]
     *   nextLevelSlices    0  0
     * </pre>
     * On level 0, all documents have an ordinal: 0 has 1, 1 has 4 and 2 has 2 as a first ordinal, this means that we need to read
     * nextLevelEntries to get the index of their ordinals on the next level. The entry for document 1 is 0, meaning that we have
     * already read all its ordinals. On the contrary 0 and 2 have more ordinals which are stored at indices 2 and 1. Let's continue
     * with document 2: it has 2 more ordinals on level 1: 3 and 4 and its next level index is 1 meaning that there are remaining
     * ordinals on the next level. On level 2 at index 1, we can read [5  0  0  0] meaning that 5 is an ordinal as well, but the
     * fact that it is followed by zeros means that there are no more ordinals. In the end, document 2 has 2, 3, 4 and 5 as ordinals.
     * <p>
     * In addition to these structures, there is another array which stores the current position (level + slice + offset in the slice)
     * in order to be able to append data in constant time.
     */
    private static class OrdinalsStore {

        private static final int PAGE_SIZE = 1 << 12;

        /**
         * Number of slots at <code>level</code>
         */
        private static int numSlots(int level) {
            return 1 << level;
        }

        private static int slotsMask(int level) {
            return numSlots(level) - 1;
        }

        /**
         * Encode the position for the given level and offset. The idea is to encode the level using unary coding in the lower bits and
         * then the offset in the higher bits.
         */
        private static long position(int level, long offset) {
            assert level >= 1;
            return (1 << (level - 1)) | (offset << level);
        }

        /**
         * Decode the level from an encoded position.
         */
        private static int level(long position) {
            return 1 + Long.numberOfTrailingZeros(position);
        }

        /**
         * Decode the offset from the position.
         */
        private static long offset(long position, int level) {
            return position >>> level;
        }

        /**
         * Get the ID of the slice given an offset.
         */
        private static long sliceID(int level, long offset) {
            return offset >>> level;
        }

        /**
         * Compute the first offset of the given slice.
         */
        private static long startOffset(int level, long slice) {
            return slice << level;
        }

        /**
         * Compute the number of ordinals stored for a value given its current position.
         */
        private static int numOrdinals(int level, long offset) {
            return (1 << level) + (int) (offset & slotsMask(level));
        }

        // Current position
        private final PagedGrowableWriter positions;
        // First level (0) of ordinals and pointers to the next level
        private final GrowableWriter firstOrdinals;
        private PagedGrowableWriter firstNextLevelSlices;
        // Ordinals and pointers for other levels +1
        private final PagedGrowableWriter[] ordinals;
        private final PagedGrowableWriter[] nextLevelSlices;
        private final int[] sizes;

        private final int startBitsPerValue;
        private final float acceptableOverheadRatio;

        OrdinalsStore(int maxDoc, int startBitsPerValue, float acceptableOverheadRatio) {
            this.startBitsPerValue = startBitsPerValue;
            this.acceptableOverheadRatio = acceptableOverheadRatio;
            positions = new PagedGrowableWriter(maxDoc, PAGE_SIZE, startBitsPerValue, acceptableOverheadRatio);
            firstOrdinals = new GrowableWriter(startBitsPerValue, maxDoc, acceptableOverheadRatio);
            // over allocate in order to never worry about the array sizes, 24 entries would allow
            // to store several millions of ordinals per doc...
            ordinals = new PagedGrowableWriter[24];
            nextLevelSlices = new PagedGrowableWriter[24];
            sizes = new int[24];
            Arrays.fill(sizes, 1); // reserve the 1st slice on every level
        }

        /**
         * Allocate a new slice and return its ID.
         */
        private long newSlice(int level) {
            final long newSlice = sizes[level]++;
            // Lazily allocate ordinals
            if (ordinals[level] == null) {
                ordinals[level] = new PagedGrowableWriter(8L * numSlots(level), PAGE_SIZE, startBitsPerValue, acceptableOverheadRatio);
            } else {
                ordinals[level] = ordinals[level].grow(sizes[level] * numSlots(level));
                if (nextLevelSlices[level] != null) {
                    nextLevelSlices[level] = nextLevelSlices[level].grow(sizes[level]);
                }
            }
            return newSlice;
        }

        public int addOrdinal(int docID, long ordinal) {
            final long position = positions.get(docID);
            if (position == 0L) { // on the first level
                return firstLevel(docID, ordinal);
            } else {
                return nonFirstLevel(docID, ordinal, position);
            }
        }

        private int firstLevel(int docID, long ordinal) {
            // 0 or 1 ordinal
            if (firstOrdinals.get(docID) == 0L) {
                firstOrdinals.set(docID, ordinal + 1);
                return 1;
            } else {
                final long newSlice = newSlice(1);
                if (firstNextLevelSlices == null) {
                    firstNextLevelSlices = new PagedGrowableWriter(firstOrdinals.size(), PAGE_SIZE, 3, acceptableOverheadRatio);
                }
                firstNextLevelSlices.set(docID, newSlice);
                final long offset = startOffset(1, newSlice);
                ordinals[1].set(offset, ordinal + 1);
                positions.set(docID, position(1, offset)); // current position is on the 1st level and not allocated yet
                return 2;
            }
        }

        private int nonFirstLevel(int docID, long ordinal, long position) {
            int level = level(position);
            long offset = offset(position, level);
            assert offset != 0L;
            if (((offset + 1) & slotsMask(level)) == 0L) {
                // reached the end of the slice, allocate a new one on the next level
                final long newSlice = newSlice(level + 1);
                if (nextLevelSlices[level] == null) {
                    nextLevelSlices[level] = new PagedGrowableWriter(sizes[level], PAGE_SIZE, 1, acceptableOverheadRatio);
                }
                nextLevelSlices[level].set(sliceID(level, offset), newSlice);
                ++level;
                offset = startOffset(level, newSlice);
                assert (offset & slotsMask(level)) == 0L;
            } else {
                // just go to the next slot
                ++offset;
            }
            ordinals[level].set(offset, ordinal + 1);
            final long newPosition = position(level, offset);
            positions.set(docID, newPosition);
            return numOrdinals(level, offset);
        }

        public void appendOrdinals(int docID, LongsRef ords) {
            // First level
            final long firstOrd = firstOrdinals.get(docID);
            if (firstOrd == 0L) {
                return;
            }
            ords.longs = ArrayUtil.grow(ords.longs, ords.offset + ords.length + 1);
            ords.longs[ords.offset + ords.length++] = firstOrd - 1;
            if (firstNextLevelSlices == null) {
                return;
            }
            long sliceID = firstNextLevelSlices.get(docID);
            if (sliceID == 0L) {
                return;
            }
            // Other levels
            for (int level = 1;; ++level) {
                final int numSlots = numSlots(level);
                ords.longs = ArrayUtil.grow(ords.longs, ords.offset + ords.length + numSlots);
                final long offset = startOffset(level, sliceID);
                for (int j = 0; j < numSlots; ++j) {
                    final long ord = ordinals[level].get(offset + j);
                    if (ord == 0L) {
                        return;
                    }
                    ords.longs[ords.offset + ords.length++] = ord - 1;
                }
                if (nextLevelSlices[level] == null) {
                    return;
                }
                sliceID = nextLevelSlices[level].get(sliceID);
                if (sliceID == 0L) {
                    return;
                }
            }
        }

    }

    private final int maxDoc;
    private long currentOrd = -1;
    private int numDocsWithValue = 0;
    private int numMultiValuedDocs = 0;
    private int totalNumOrds = 0;

    private OrdinalsStore ordinals;
    private final LongsRef spare;

    public OrdinalsBuilder(int maxDoc, float acceptableOverheadRatio) {
        this.maxDoc = maxDoc;
        int startBitsPerValue = 8;
        ordinals = new OrdinalsStore(maxDoc, startBitsPerValue, acceptableOverheadRatio);
        spare = new LongsRef();
    }

    public OrdinalsBuilder(int maxDoc) {
        this(maxDoc, DEFAULT_ACCEPTABLE_OVERHEAD_RATIO);
    }

    /**
     * Returns a shared {@link LongsRef} instance for the given doc ID holding all ordinals associated with it.
     */
    public LongsRef docOrds(int docID) {
        spare.offset = spare.length = 0;
        ordinals.appendOrdinals(docID, spare);
        return spare;
    }

    /**
     * Return a {@link PackedInts.Reader} instance mapping every doc ID to its first ordinal + 1 if it exists and 0 otherwise.
     */
    public PackedInts.Reader getFirstOrdinals() {
        return ordinals.firstOrdinals;
    }

    /**
     * Advances the {@link OrdinalsBuilder} to the next ordinal and
     * return the current ordinal.
     */
    public long nextOrdinal() {
        return ++currentOrd;
    }

    /**
     * Returns the current ordinal or {@code 0} if this build has not been advanced via
     * {@link #nextOrdinal()}.
     */
    public long currentOrdinal() {
        return currentOrd;
    }

    /**
     * Associates the given document id with the current ordinal.
     */
    public OrdinalsBuilder addDoc(int doc) {
        totalNumOrds++;
        final int numValues = ordinals.addOrdinal(doc, currentOrd);
        if (numValues == 1) {
            ++numDocsWithValue;
        } else if (numValues == 2) {
            ++numMultiValuedDocs;
        }
        return this;
    }

    /**
     * Returns the number distinct of document IDs associated with two or more values.
     */
    public int getNumMultiValuesDocs() {
        return numMultiValuedDocs;
    }

    /**
     * Returns the number of document ID to ordinal pairs in this builder.
     */
    public int getTotalNumOrds() {
        return totalNumOrds;
    }

    /**
     * Returns the number of distinct ordinals in this builder.
     */
    public long getValueCount() {
        return currentOrd + 1;
    }

    /**
     * Builds an {@link Ordinals} instance from the builders current state.
     */
    public Ordinals build() {
        final float acceptableOverheadRatio = PackedInts.DEFAULT;
        if (numMultiValuedDocs > 0
            || MultiOrdinals.significantlySmallerThanSinglePackedOrdinals(
                maxDoc,
                numDocsWithValue,
                getValueCount(),
                acceptableOverheadRatio
            )) {
            // MultiOrdinals can be smaller than SinglePackedOrdinals for sparse fields
            return new MultiOrdinals(this, acceptableOverheadRatio);
        } else {
            return new SinglePackedOrdinals(this, acceptableOverheadRatio);
        }
    }

    /**
     * Returns the maximum document ID this builder can associate with an ordinal
     */
    public int maxDoc() {
        return maxDoc;
    }

    /**
     * Closes this builder and release all resources.
     */
    @Override
    public void close() {
        ordinals = null;
    }
}
