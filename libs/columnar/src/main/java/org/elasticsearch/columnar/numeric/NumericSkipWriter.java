/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Writes a numeric column's doc-values skip index: documents are grouped into intervals, and for each
 * interval the value bounds and doc-id range are recorded. Intervals are aggregated into a small tree
 * so a range query can skip whole subtrees whose bounds miss the query.
 *
 * <p>An interval normally holds {@link #INTERVAL_SIZE} documents, but a dense run of one constant
 * value is kept together in a single (larger) interval so constant columns stay maximally skippable.
 * Level-0 entries are a fixed 29 bytes, which is what lets {@link NumericColumnSkipper} jump a level in
 * a constant number of bytes.
 */
public final class NumericSkipWriter {

    /** Documents per skip interval (before the constant-run extension). */
    public static final int INTERVAL_SIZE = 4096;
    /** Number of levels in the skip tree. */
    public static final int MAX_LEVEL = 4;
    /** Intervals grouped per level, as a bit shift. */
    public static final int LEVEL_SHIFT = 3;

    private static final int MAX_ACCUMULATORS = 1 << (LEVEL_SHIFT * (MAX_LEVEL - 1));

    private NumericSkipWriter() {}

    /**
     * Streams {@code values} (in doc order) and appends the skip index to {@code data}, returning its
     * position and the column-wide summary. Accumulators are held only one aggregation group at a
     * time, so nothing document-proportional stays on the heap.
     */
    public static NumericColumnMetadata.Skipper write(NumericColumnValues values, IndexOutput data) throws IOException {
        final long start = data.getFilePointer();
        long globalMaxValue = Long.MIN_VALUE;
        long globalMinValue = Long.MAX_VALUE;
        int globalDocCount = 0;
        int maxDocId = -1;
        int globalMaxValueCount = 0;
        final List<SkipAccumulator> accumulators = new ArrayList<>();
        SkipAccumulator accumulator = null;
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            final long firstValue = values.nextValue();
            final int valueCount = values.valueCount();
            globalMaxValueCount = Math.max(globalMaxValueCount, valueCount);
            if (accumulator != null && accumulator.isDone(valueCount, firstValue, doc)) {
                globalMaxValue = Math.max(globalMaxValue, accumulator.maxValue);
                globalMinValue = Math.min(globalMinValue, accumulator.minValue);
                globalDocCount += accumulator.docCount;
                maxDocId = accumulator.maxDocID;
                accumulator = null;
                if (accumulators.size() == MAX_ACCUMULATORS) {
                    writeLevels(accumulators, data);
                    accumulators.clear();
                }
            }
            if (accumulator == null) {
                accumulator = new SkipAccumulator(doc);
                accumulators.add(accumulator);
            }
            accumulator.nextDoc(doc);
            accumulator.accumulate(firstValue);
            for (int i = 1; i < valueCount; i++) {
                accumulator.accumulate(values.nextValue());
            }
        }
        if (accumulators.isEmpty() == false) {
            globalMaxValue = Math.max(globalMaxValue, accumulator.maxValue);
            globalMinValue = Math.min(globalMinValue, accumulator.minValue);
            globalDocCount += accumulator.docCount;
            maxDocId = accumulator.maxDocID;
            writeLevels(accumulators, data);
        }
        return new NumericColumnMetadata.Skipper(
            start,
            data.getFilePointer() - start,
            globalMinValue,
            globalMaxValue,
            globalDocCount,
            maxDocId,
            globalMaxValueCount
        );
    }

    private static void writeLevels(List<SkipAccumulator> accumulators, IndexOutput data) throws IOException {
        final List<List<SkipAccumulator>> levels = new ArrayList<>(MAX_LEVEL);
        levels.add(accumulators);
        for (int i = 0; i < MAX_LEVEL - 1; i++) {
            levels.add(buildLevel(levels.get(i)));
        }
        final int total = accumulators.size();
        for (int index = 0; index < total; index++) {
            final int entryLevels = levelsAt(index, total);
            data.writeByte((byte) entryLevels);
            for (int level = entryLevels - 1; level >= 0; level--) {
                final SkipAccumulator acc = levels.get(level).get(index >> (LEVEL_SHIFT * level));
                data.writeInt(acc.maxDocID);
                data.writeInt(acc.minDocID);
                data.writeLong(acc.maxValue);
                data.writeLong(acc.minValue);
                data.writeInt(acc.docCount);
            }
        }
    }

    private static List<SkipAccumulator> buildLevel(List<SkipAccumulator> accumulators) {
        final int levelSize = 1 << LEVEL_SHIFT;
        final List<SkipAccumulator> collector = new ArrayList<>();
        for (int i = 0; i < accumulators.size() - levelSize + 1; i += levelSize) {
            collector.add(SkipAccumulator.merge(accumulators, i, levelSize));
        }
        return collector;
    }

    private static int levelsAt(int index, int size) {
        if (Integer.numberOfTrailingZeros(index) >= LEVEL_SHIFT) {
            final int left = size - index;
            for (int level = MAX_LEVEL - 1; level > 0; level--) {
                final int intervals = 1 << (LEVEL_SHIFT * level);
                if (left >= intervals && index % intervals == 0) {
                    return level + 1;
                }
            }
        }
        return 1;
    }

    private static final class SkipAccumulator {
        int minDocID;
        int maxDocID;
        int docCount;
        long minValue = Long.MAX_VALUE;
        long maxValue = Long.MIN_VALUE;

        SkipAccumulator(int docID) {
            minDocID = docID;
        }

        boolean isDone(int valueCount, long nextValue, int nextDoc) {
            if (docCount < INTERVAL_SIZE) {
                return false;
            }
            // Keep extending only while the interval stays a single dense constant run.
            return valueCount > 1 || minValue != maxValue || minValue != nextValue || docCount != nextDoc - minDocID;
        }

        void accumulate(long value) {
            minValue = Math.min(minValue, value);
            maxValue = Math.max(maxValue, value);
        }

        void accumulate(SkipAccumulator other) {
            maxDocID = other.maxDocID;
            minValue = Math.min(minValue, other.minValue);
            maxValue = Math.max(maxValue, other.maxValue);
            docCount += other.docCount;
        }

        void nextDoc(int docID) {
            maxDocID = docID;
            docCount++;
        }

        static SkipAccumulator merge(List<SkipAccumulator> list, int index, int length) {
            final SkipAccumulator acc = new SkipAccumulator(list.get(index).minDocID);
            for (int i = 0; i < length; i++) {
                acc.accumulate(list.get(index + i));
            }
            return acc;
        }
    }
}
