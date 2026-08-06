/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/**
 * Opens a run table written by {@link RunTableSortedSetOrdinalWriter} as a virtual
 * {@link SortedNumericDocValues} whose {@link SortedNumericDocValues#docValueCount()} is the doc's
 * set size and whose {@link SortedNumericDocValues#nextValue()} walks the doc's ordinals ascending.
 *
 * <p>The ES95 producer wraps this into a {@code SortedSetDocValues}, pairing it with the terms
 * dictionary it reads separately, so this reader deals only with ordinals. A {@link RunCursor} is
 * positioned on the run containing the target doc, {@code setOffset[]} yields the run's slice
 * {@code [setOffset.get(run), setOffset.get(run + 1))} into the flattened {@code ordStream[]}, and
 * {@code nextValue()} walks that slice one ord at a time. Each slice is ascending, so the ordinals
 * come out strictly increasing without any per-doc work.
 *
 * <p>The view is sparse-aware. A doc whose run carries the empty set (a zero-width slice) has no
 * value: {@link SortedNumericDocValues#advanceExact} returns {@code false} for it and
 * {@link SortedNumericDocValues#nextDoc}/{@link SortedNumericDocValues#advance} skip past it to the
 * next value-bearing doc. A dense field, whose stream never contains an empty set, behaves exactly as
 * a plain dense {@link SortedNumericDocValues} where {@code advanceExact} always returns {@code true}.
 */
public final class RunTableSortedSetOrdinalReader {

    private RunTableSortedSetOrdinalReader() {}

    /**
     * Parsed run-table header. {@link SortedSetRunTableLayout#readMeta} decodes it from the meta stream
     * at segment-open time, where the {@link DirectMonotonicReader.Meta} for {@code startDoc[]} and
     * {@code setOffset[]} must be loaded, so that {@link SortedSetRunTableLayout#open} can defer building
     * the doc values view until first access without re-reading meta. The offsets are absolute in the
     * shared data stream.
     */
    public record Meta(
        int numRuns,
        int valueCount,
        int totalOrds,
        int blockShift,
        DirectMonotonicReader.Meta startDocsMeta,
        DirectMonotonicReader.Meta setOffsetsMeta,
        long dataStart,
        long startDocsLength,
        long setOffsetsLength
    ) {}

    static SortedNumericDocValues open(
        final RunCursor cursor,
        final DirectMonotonicReader setOffsets,
        final LongValues ordStream,
        int maxDoc
    ) {
        return new RunTableSortedNumericDocValues(cursor, setOffsets, ordStream, maxDoc);
    }

    private static final class RunTableSortedNumericDocValues extends SortedNumericDocValues {

        private final RunCursor cursor;
        private final DirectMonotonicReader setOffsets;
        private final LongValues ordStream;
        private final int maxDoc;

        private int doc = -1;
        private int runOrdStart = 0;
        private int runOrdEnd = 0;
        private int nextOrdIndex = 0;

        RunTableSortedNumericDocValues(
            final RunCursor cursor,
            final DirectMonotonicReader setOffsets,
            final LongValues ordStream,
            int maxDoc
        ) {
            this.cursor = cursor;
            this.setOffsets = setOffsets;
            this.ordStream = ordStream;
            this.maxDoc = maxDoc;
        }

        private void positionOnRun(int run) {
            runOrdStart = (int) setOffsets.get(run);
            runOrdEnd = (int) setOffsets.get(run + 1);
            nextOrdIndex = runOrdStart;
        }

        @Override
        public boolean advanceExact(int target) {
            cursor.seekDoc(target);
            doc = target;
            positionOnRun(cursor.run());
            // A zero-width slice is the empty set: the doc is absent.
            return runOrdEnd > runOrdStart;
        }

        @Override
        public long nextValue() {
            return ordStream.get(nextOrdIndex++);
        }

        @Override
        public int docValueCount() {
            return runOrdEnd - runOrdStart;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) {
            if (target >= maxDoc) {
                doc = NO_MORE_DOCS;
                return doc;
            }
            cursor.seekDoc(target);
            int run = cursor.run();
            positionOnRun(run);
            if (runOrdEnd > runOrdStart) {
                doc = target;
                return doc;
            }
            // The target falls in an empty (absent) run. Runs are maximal, so the next run carries a real
            // set; skip forward to its first doc. Past the last run there are no more values.
            run++;
            if (run >= cursor.numRuns()) {
                doc = NO_MORE_DOCS;
                return doc;
            }
            cursor.positionOn(run);
            positionOnRun(run);
            doc = cursor.startDoc(run);
            return doc;
        }

        @Override
        public long cost() {
            return maxDoc;
        }
    }
}
