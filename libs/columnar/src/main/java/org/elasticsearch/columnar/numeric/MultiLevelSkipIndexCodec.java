/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The default skip codec: documents are grouped into intervals, and for each interval the value bounds
 * and doc-id range are recorded. Intervals are aggregated into a small tree so a range query can skip
 * whole subtrees whose bounds miss the query.
 *
 * <p>An interval normally holds {@link #INTERVAL_SIZE} documents, but a dense run of one constant value
 * is kept together in a single (larger) interval so constant columns stay maximally skippable. Level-0
 * entries are a fixed 29 bytes, which is what lets {@link NumericColumnSkipper} jump a level in a
 * constant number of bytes.
 */
public final class MultiLevelSkipIndexCodec implements SkipIndexCodec {

    /** Documents per skip interval (before the constant-run extension). */
    public static final int INTERVAL_SIZE = 4096;
    /** Number of levels in the skip tree. */
    public static final int MAX_LEVEL = 4;
    /** Intervals grouped per level, as a bit shift. */
    public static final int LEVEL_SHIFT = 3;

    private static final int MAX_ACCUMULATORS = 1 << (LEVEL_SHIFT * (MAX_LEVEL - 1));

    @Override
    public byte id() {
        return MULTI_LEVEL_ID;
    }

    @Override
    public Writer writer() {
        return new MultiLevelWriter();
    }

    @Override
    public DocValuesSkipper reader(NumericColumnMetadata.Skipper meta, IndexInput data) throws IOException {
        return new NumericColumnSkipper(meta, data);
    }

    /** The incremental {@link Writer}; skip bytes accumulate in {@link #buffer} until {@link #finish}. */
    private static final class MultiLevelWriter implements Writer {
        private final ByteBuffersDataOutput buffer = new ByteBuffersDataOutput();
        private final List<SkipAccumulator> accumulators = new ArrayList<>();
        private SkipAccumulator accumulator;

        private long globalMaxValue = Long.MIN_VALUE;
        private long globalMinValue = Long.MAX_VALUE;
        private int globalDocCount = 0;
        private int maxDocId = -1;
        private int globalMaxValueCount = 0;

        private int currentDoc;
        private int currentValueCount;
        private int valueIndex;

        @Override
        public void startDoc(int doc, int valueCount) {
            currentDoc = doc;
            currentValueCount = valueCount;
            valueIndex = 0;
            globalMaxValueCount = Math.max(globalMaxValueCount, valueCount);
        }

        @Override
        public void add(long value) {
            if (valueIndex == 0) {
                // The interval boundary is decided from the document's first value.
                if (accumulator != null && accumulator.isDone(currentValueCount, value, currentDoc)) {
                    globalMaxValue = Math.max(globalMaxValue, accumulator.maxValue);
                    globalMinValue = Math.min(globalMinValue, accumulator.minValue);
                    globalDocCount += accumulator.docCount;
                    maxDocId = accumulator.maxDocID;
                    accumulator = null;
                    if (accumulators.size() == MAX_ACCUMULATORS) {
                        try {
                            writeLevels(accumulators, buffer);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                        accumulators.clear();
                    }
                }
                if (accumulator == null) {
                    accumulator = new SkipAccumulator(currentDoc);
                    accumulators.add(accumulator);
                }
                accumulator.nextDoc(currentDoc);
            }
            accumulator.accumulate(value);
            valueIndex++;
        }

        @Override
        public NumericColumnMetadata.Skipper finish(IndexOutput data) throws IOException {
            if (accumulators.isEmpty() == false) {
                globalMaxValue = Math.max(globalMaxValue, accumulator.maxValue);
                globalMinValue = Math.min(globalMinValue, accumulator.minValue);
                globalDocCount += accumulator.docCount;
                maxDocId = accumulator.maxDocID;
                writeLevels(accumulators, buffer);
            }
            final long start = data.getFilePointer();
            buffer.copyTo(data);
            return new NumericColumnMetadata.Skipper(
                start,
                data.getFilePointer() - start,
                globalMinValue,
                globalMaxValue,
                globalDocCount,
                maxDocId,
                globalMaxValueCount,
                MULTI_LEVEL_ID
            );
        }
    }

    private static void writeLevels(List<SkipAccumulator> accumulators, DataOutput data) throws IOException {
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
