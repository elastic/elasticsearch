/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Skip index codec for plain-layout string columns. Documents are grouped into intervals and for each
 * interval the min and max byte values plus the doc-id range are recorded. Intervals are aggregated
 * into a tree with a configurable number of levels and branching factor, so a range query can skip whole
 * subtrees whose bounds miss the query. The defaults match the numeric codec geometry.
 *
 * <p>Unlike the numeric codec, entries are variable-length because {@code BytesRef} values have variable
 * length, so a reader cannot jump a subtree by a constant number of bytes. Every level record instead
 * carries the length of the group of entries it covers, which lets {@link PlainStringColumnSkipper} skip a
 * subtree the same way, by seeking past it.
 *
 * <p>An entry is a level count, then one record per level from the coarsest down to level 0:
 * {@code maxDocId}, the byte length of the group of entries the record covers counted from the entry's
 * first byte, {@code minDocId}, {@code docCount}, then the min and max term, each as a vint length and
 * the bytes.
 *
 * <p>{@code Plain.Skipper} records the number of levels and the bound cap. The levels size the reader's
 * cursor. The cap tells whether a bound is exact: no value longer than it was cut. The interval size and
 * the branching factor shape what is written and are not needed to read it.
 *
 * <p>Bounds prune only where values have locality in document order. The writer measures it on the first few
 * intervals, as the share of interval pairs whose ranges overlap, and writes no region and stops comparing
 * for a column where that share reaches the limit, since a range could skip almost nothing. A column the
 * reader bisects is given no region either.
 */
final class PlainSkipIndexCodec {

    /** Frozen id for this codec. Never reuse or repurpose. */
    static final byte ID = 0;

    static final int DEFAULT_INTERVAL_SIZE = 4096;
    static final int DEFAULT_MAX_LEVEL = 4;
    static final int DEFAULT_LEVEL_SHIFT = 3;

    /**
     * Most bytes of a term kept as a bound. A longer value is cut: the min to its prefix, which sorts at or
     * before it, and the max to its prefix with the last byte that can be raised raised, which sorts after
     * it. A bound only ever widens, so a range is never wrongly ruled out. Text values have no length
     * limit, and without one a batch of 585 records could hold gigabytes of bounds.
     */
    static final int DEFAULT_MAX_BOUND_BYTES = 256;

    /**
     * Share of interval pairs with overlapping ranges at which a column gets no region. 1 means every
     * interval overlaps every other, as in a column of random values.
     */
    static final double DEFAULT_MAX_OVERLAP_FRACTION = 0.90;

    /** Intervals the writer sees before it checks the overlap, so a column without locality stops paying early. */
    static final int DEFAULT_LOCALITY_SAMPLE_INTERVALS = 16;

    static final PlainSkipIndexCodec DEFAULT = new PlainSkipIndexCodec(
        DEFAULT_INTERVAL_SIZE,
        DEFAULT_MAX_LEVEL,
        DEFAULT_LEVEL_SHIFT,
        DEFAULT_MAX_BOUND_BYTES,
        DEFAULT_MAX_OVERLAP_FRACTION,
        DEFAULT_LOCALITY_SAMPLE_INTERVALS
    );

    private static final int FIXED_LEVEL_BYTES = 4 * Integer.BYTES;

    private final int intervalSize;
    private final int maxLevel;
    private final int levelShift;
    private final int maxBoundBytes;
    private final double maxOverlapFraction;
    private final int localitySampleIntervals;
    private final int intervalsPerBatch;

    /**
     * @param intervalSize  documents with a slot in one level-0 interval
     * @param maxLevel      levels in the tree, level 0 being the intervals themselves
     * @param levelShift    log2 of how many intervals of one level a record of the next level covers
     * @param maxBoundBytes longest min or max term kept for an interval, longer ones are widened to fit
     * @param maxOverlapFraction share of interval pairs with overlapping ranges at which a column gets no
     *                      region; above 1 no column is ever left without one for its overlap
     * @param localitySampleIntervals intervals the writer sees before it checks the overlap, at least 2; a batch
     *                      smaller than that is checked when it is full, and one interval cannot show locality
     */
    PlainSkipIndexCodec(
        int intervalSize,
        int maxLevel,
        int levelShift,
        int maxBoundBytes,
        double maxOverlapFraction,
        int localitySampleIntervals
    ) {
        if (intervalSize < 1) {
            throw new IllegalArgumentException("intervalSize must be positive, got [" + intervalSize + "]");
        }
        if (maxLevel < 1) {
            throw new IllegalArgumentException("maxLevel must be positive, got [" + maxLevel + "]");
        }
        if (levelShift < 1) {
            throw new IllegalArgumentException("levelShift must be positive, got [" + levelShift + "]");
        }
        if ((long) levelShift * (maxLevel - 1) > 30) {
            throw new IllegalArgumentException("levelShift [" + levelShift + "] over [" + maxLevel + "] levels covers too many intervals");
        }
        if (maxBoundBytes < 1) {
            throw new IllegalArgumentException("maxBoundBytes must be positive, got [" + maxBoundBytes + "]");
        }
        if (Double.isNaN(maxOverlapFraction) || maxOverlapFraction <= 0) {
            throw new IllegalArgumentException("maxOverlapFraction must be positive, got [" + maxOverlapFraction + "]");
        }
        if (localitySampleIntervals < 2) {
            throw new IllegalArgumentException("localitySampleIntervals must be at least 2, got [" + localitySampleIntervals + "]");
        }
        this.intervalSize = intervalSize;
        this.maxLevel = maxLevel;
        this.levelShift = levelShift;
        this.maxBoundBytes = maxBoundBytes;
        this.maxOverlapFraction = maxOverlapFraction;
        this.intervalsPerBatch = 1 << (levelShift * (maxLevel - 1));
        this.localitySampleIntervals = Math.min(localitySampleIntervals, intervalsPerBatch);
    }

    /** A writer appending this codec's entries to {@code skipOutput}, which nothing else may write to meanwhile. */
    Writer writer(IndexOutput skipOutput) {
        return new Writer(skipOutput, false);
    }

    /** A writer for a column that gets no region: it takes the same calls, keeps nothing and writes nothing. */
    Writer disabledWriter() {
        return new Writer(null, true);
    }

    /**
     * Accumulates per-document values during the plain write pass and writes the interval entries to the
     * skip-index output as intervals complete. Nothing else writes to that output while a column is being
     * written, so the entries go straight to it.
     */
    final class Writer {
        private final IndexOutput skipOutput;
        private final long regionStart;
        private final List<IntervalBounds> pendingIntervals = new ArrayList<>();
        private IntervalBounds currentInterval;
        private int lastDocId = -1;
        private boolean localityChecked;
        private boolean entriesWritten;
        private boolean regionDropped;

        private Writer(IndexOutput skipOutput, boolean regionDropped) {
            this.skipOutput = skipOutput;
            this.regionStart = regionDropped ? 0 : skipOutput.getFilePointer();
            this.regionDropped = regionDropped;
        }

        /** Called once per document before its values are delivered via {@link #add}. */
        void startDoc(int doc) throws IOException {
            if (regionDropped) {
                return;
            }
            if (currentInterval != null && currentInterval.isFull(intervalSize)) {
                pendingIntervals.add(currentInterval);
                currentInterval = null;
                if (localityChecked == false && pendingIntervals.size() == localitySampleIntervals) {
                    localityChecked = true;
                    if (lacksLocality()) {
                        dropRegion();
                        return;
                    }
                }
                if (pendingIntervals.size() == intervalsPerBatch) {
                    writeEntries(pendingIntervals, skipOutput);
                    pendingIntervals.clear();
                    entriesWritten = true;
                }
            }
            if (currentInterval == null) {
                currentInterval = new IntervalBounds(doc, maxBoundBytes);
            }
            currentInterval.addDoc(doc);
            lastDocId = doc;
        }

        /**
         * One non-null value of the current document. Nulls must not be passed here; they do not
         * contribute to byte-range bounds and would force the min to the empty string.
         */
        void add(BytesRef value) {
            if (regionDropped == false) {
                currentInterval.includeValue(value);
            }
        }

        /**
         * Writes what is left and returns where the region is, or {@code null} if no document was seen or the
         * column gets no region.
         *
         * @param readerBisects whether the reader finds terms in this column by bisection, which never reads a region
         */
        StringColumnMetadata.Plain.Skipper finish(boolean readerBisects) throws IOException {
            if (regionDropped) {
                return null;
            }
            if (currentInterval != null) {
                pendingIntervals.add(currentInterval);
                currentInterval = null;
            }
            if (pendingIntervals.isEmpty()) {
                return null;
            }
            if (entriesWritten == false && (readerBisects || (localityChecked == false && lacksLocality()))) {
                dropRegion();
                return null;
            }
            writeEntries(pendingIntervals, skipOutput);
            return new StringColumnMetadata.Plain.Skipper(
                regionStart,
                skipOutput.getFilePointer() - regionStart,
                lastDocId,
                maxLevel,
                maxBoundBytes,
                ID
            );
        }

        private boolean lacksLocality() {
            return overlapFraction(pendingIntervals) >= maxOverlapFraction;
        }

        private void dropRegion() {
            regionDropped = true;
            pendingIntervals.clear();
            currentInterval = null;
        }
    }

    /**
     * Share of ordered interval pairs, an interval with itself included, whose [min, max] ranges overlap; 1 when no
     * interval holds a term.
     */
    private static double overlapFraction(List<IntervalBounds> intervals) {
        final int intervalCount = intervals.size();
        long overlappingPairs = 0;
        for (int i = 0; i < intervalCount; i++) {
            final IntervalBounds first = intervals.get(i);
            if (first.hasTerm == false) {
                continue;
            }
            overlappingPairs++;
            for (int j = i + 1; j < intervalCount; j++) {
                final IntervalBounds second = intervals.get(j);
                if (second.hasTerm
                    && first.minTerm.get().compareTo(second.maxTerm.get()) <= 0
                    && second.minTerm.get().compareTo(first.maxTerm.get()) <= 0) {
                    overlappingPairs += 2;
                }
            }
        }
        return overlappingPairs == 0 ? 1 : overlappingPairs / ((double) intervalCount * intervalCount);
    }

    private void writeEntries(List<IntervalBounds> intervals, IndexOutput output) throws IOException {
        final List<List<IntervalBounds>> levels = new ArrayList<>(maxLevel);
        levels.add(intervals);
        for (int i = 0; i < maxLevel - 1; i++) {
            levels.add(aggregateLevel(levels.get(i)));
        }
        final int intervalCount = intervals.size();
        final int[] entryBytes = new int[intervalCount];
        for (int index = 0; index < intervalCount; index++) {
            int entryLength = 1;
            for (int level = levelsInEntry(index, intervalCount) - 1; level >= 0; level--) {
                entryLength += levels.get(level).get(index >> (levelShift * level)).encodedLength();
            }
            entryBytes[index] = entryLength;
        }
        for (int index = 0; index < intervalCount; index++) {
            final int numLevels = levelsInEntry(index, intervalCount);
            output.writeByte((byte) numLevels);
            for (int level = numLevels - 1; level >= 0; level--) {
                final IntervalBounds bounds = levels.get(level).get(index >> (levelShift * level));
                long groupBytes = 0;
                for (int member = index; member < index + (1 << (levelShift * level)); member++) {
                    groupBytes += entryBytes[member];
                }
                output.writeInt(bounds.maxDocId);
                output.writeInt(Math.toIntExact(groupBytes));
                output.writeInt(bounds.minDocId);
                output.writeInt(bounds.docCount);
                final BytesRef min = bounds.hasTerm ? bounds.minTerm.get() : new BytesRef();
                final BytesRef max = bounds.hasTerm ? bounds.maxTerm.get() : new BytesRef();
                output.writeVInt(min.length);
                output.writeBytes(min.bytes, min.offset, min.length);
                output.writeVInt(max.length);
                output.writeBytes(max.bytes, max.offset, max.length);
            }
        }
    }

    private List<IntervalBounds> aggregateLevel(List<IntervalBounds> intervals) {
        final int branching = 1 << levelShift;
        final List<IntervalBounds> level = new ArrayList<>();
        for (int i = 0; i < intervals.size() - branching + 1; i += branching) {
            level.add(IntervalBounds.union(intervals, i, branching, maxBoundBytes));
        }
        return level;
    }

    private int levelsInEntry(int index, int intervalCount) {
        if (Integer.numberOfTrailingZeros(index) >= levelShift) {
            final int intervalsLeft = intervalCount - index;
            for (int level = maxLevel - 1; level > 0; level--) {
                final int intervalsPerRecord = 1 << (levelShift * level);
                if (intervalsLeft >= intervalsPerRecord && index % intervalsPerRecord == 0) {
                    return level + 1;
                }
            }
        }
        return 1;
    }

    private static final class IntervalBounds {
        final int maxBoundBytes;
        int minDocId;
        int maxDocId;
        int docCount;
        final BytesRefBuilder minTerm = new BytesRefBuilder();
        final BytesRefBuilder maxTerm = new BytesRefBuilder();
        boolean hasTerm;
        long minTermPrefix;
        long maxTermPrefix;

        IntervalBounds(int doc, int maxBoundBytes) {
            this.minDocId = doc;
            this.maxBoundBytes = maxBoundBytes;
        }

        /** Bytes of this interval's record in an entry. */
        int encodedLength() {
            final int min = hasTerm ? minTerm.length() : 0;
            final int max = hasTerm ? maxTerm.length() : 0;
            return FIXED_LEVEL_BYTES + vIntLength(min) + min + vIntLength(max) + max;
        }

        boolean isFull(int intervalSize) {
            return docCount >= intervalSize;
        }

        void addDoc(int doc) {
            maxDocId = doc;
            docCount++;
        }

        // NOTE: zero-padded, so two prefixes that differ settle the order, and only a tie reads the whole terms.
        void includeValue(BytesRef value) {
            final long prefix = leadingBytes(value);
            if (hasTerm == false) {
                setMin(value);
                setMax(value);
                minTermPrefix = leadingBytes(minTerm.get());
                maxTermPrefix = leadingBytes(maxTerm.get());
                hasTerm = true;
            } else if (compareWithBound(prefix, value, maxTermPrefix, maxTerm.get()) > 0) {
                setMax(value);
                maxTermPrefix = leadingBytes(maxTerm.get());
            } else if (compareWithBound(prefix, value, minTermPrefix, minTerm.get()) < 0) {
                setMin(value);
                minTermPrefix = leadingBytes(minTerm.get());
            }
        }

        private static int compareWithBound(long prefix, BytesRef value, long boundPrefix, BytesRef bound) {
            final int byPrefix = Long.compareUnsigned(prefix, boundPrefix);
            return byPrefix != 0 ? byPrefix : value.compareTo(bound);
        }

        private static long leadingBytes(BytesRef term) {
            if (term.length >= Long.BYTES) {
                return (long) BitUtil.VH_BE_LONG.get(term.bytes, term.offset);
            }
            long prefix = 0;
            for (int i = 0; i < term.length; i++) {
                prefix |= (term.bytes[term.offset + i] & 0xFFL) << (Long.SIZE - Byte.SIZE * (i + 1));
            }
            return prefix;
        }

        private void setMin(BytesRef value) {
            minTerm.copyBytes(value.bytes, value.offset, Math.min(value.length, maxBoundBytes));
        }

        private void setMax(BytesRef value) {
            if (value.length <= maxBoundBytes) {
                maxTerm.copyBytes(value);
                return;
            }
            for (int at = maxBoundBytes - 1; at >= 0; at--) {
                final byte raised = (byte) (value.bytes[value.offset + at] + 1);
                if (raised != 0) {
                    maxTerm.copyBytes(value.bytes, value.offset, at);
                    maxTerm.append(raised);
                    return;
                }
            }
            maxTerm.copyBytes(value);
        }

        void includeBounds(IntervalBounds other) {
            maxDocId = other.maxDocId;
            docCount += other.docCount;
            if (other.hasTerm) {
                if (hasTerm == false) {
                    minTerm.copyBytes(other.minTerm.get());
                    maxTerm.copyBytes(other.maxTerm.get());
                    hasTerm = true;
                } else {
                    if (other.minTerm.get().compareTo(minTerm.get()) < 0) {
                        minTerm.copyBytes(other.minTerm.get());
                    }
                    if (other.maxTerm.get().compareTo(maxTerm.get()) > 0) {
                        maxTerm.copyBytes(other.maxTerm.get());
                    }
                }
            }
        }

        static IntervalBounds union(List<IntervalBounds> list, int index, int length, int maxBoundBytes) {
            final IntervalBounds acc = new IntervalBounds(list.get(index).minDocId, maxBoundBytes);
            for (int i = 0; i < length; i++) {
                acc.includeBounds(list.get(index + i));
            }
            return acc;
        }
    }

    private static int vIntLength(int value) {
        return (38 - Integer.numberOfLeadingZeros(value | 1)) / 7;
    }
}
