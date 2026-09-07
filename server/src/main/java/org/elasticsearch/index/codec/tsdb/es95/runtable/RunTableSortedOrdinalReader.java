/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectReader;
import org.elasticsearch.index.codec.tsdb.SortedRunView;

/**
 * Opens a run table written by {@link RunTableSortedOrdinalWriter} as a virtual
 * {@link NumericDocValues} whose {@link NumericDocValues#longValue()} returns the doc's ordinal.
 *
 * <p>The ES95 producer wraps this into a {@code SortedDocValues}, pairing it with the terms
 * dictionary it reads separately, so this reader deals only with ordinals. Per-doc ordinals are
 * reconstructed from the run columns: a {@link Cursor} is positioned on the run containing the
 * target doc and {@code longValue()} reads {@code ordPerRun[cursor.run()]} off-heap on demand through
 * a {@link DirectReader} over a random-access slice of the mapped data input.
 *
 * <p>The view is sparse-aware. A doc whose run carries the reserved sentinel ordinal (equal to the
 * field cardinality {@code K}) has no value: {@link NumericDocValues#advanceExact} returns {@code false}
 * for it and {@link NumericDocValues#nextDoc}/{@link NumericDocValues#advance} skip past it to the next
 * value-bearing doc. A dense field, whose stream never contains the sentinel, behaves exactly as a plain
 * dense {@link NumericDocValues} where {@code advanceExact} always returns {@code true}.
 */
public final class RunTableSortedOrdinalReader {

    private RunTableSortedOrdinalReader() {}

    /**
     * Parsed run-table header. {@link SortedRunTableLayout#readMeta} decodes it from the meta stream at
     * segment-open time, where the {@link DirectMonotonicReader.Meta} for {@code startDoc[]} must be
     * loaded, so that {@link SortedRunTableLayout#open} can defer building the doc values view until first
     * access without re-reading meta. The offsets are absolute in the shared data stream.
     *
     * @param numRuns         number of runs in the table
     * @param valueCount      field cardinality K; the absent sentinel ordinal equals this value
     * @param maxOrd          largest ordinal actually written including the sentinel, sizing the bits per run ordinal
     * @param blockShift      block shift of the {@code startDoc[]} {@link DirectMonotonicReader}
     * @param startDocsMeta   {@code startDoc[]} monotonic reader metadata
     * @param dataStart       absolute data-stream offset where {@code startDoc[]} begins
     * @param startDocsLength byte length of the {@code startDoc[]} data section
     */
    public record Meta(
        int numRuns,
        int valueCount,
        int maxOrd,
        int blockShift,
        DirectMonotonicReader.Meta startDocsMeta,
        long dataStart,
        long startDocsLength
    ) {}

    static NumericDocValues open(final Cursor cursor, final LongValues ordsPerRun, int maxDoc, int sentinel) {
        return new RunTableNumericDocValues(cursor, ordsPerRun, maxDoc, sentinel);
    }

    /**
     * Run-table implementation of {@link SortedRunView}. Named locally so the layout and reader continue to refer
     * to it as {@code Runs}; the base interface is what the producer hands to the merge path.
     */
    public interface Runs extends SortedRunView {}

    static Runs openRuns(final LongValues startDocs, final LongValues ordsPerRun, int numRuns, int maxDoc, int valueCount) {
        return new RunTableRuns(startDocs, ordsPerRun, numRuns, maxDoc, valueCount);
    }

    private static final class RunTableRuns implements Runs {

        private final LongValues startDocs;
        private final LongValues ordsPerRun;
        private final int numRuns;
        private final int maxDoc;
        private final int valueCount;

        RunTableRuns(final LongValues startDocs, final LongValues ordsPerRun, int numRuns, int maxDoc, int valueCount) {
            this.startDocs = startDocs;
            this.ordsPerRun = ordsPerRun;
            this.numRuns = numRuns;
            this.maxDoc = maxDoc;
            this.valueCount = valueCount;
        }

        @Override
        public int count() {
            return numRuns;
        }

        @Override
        public int startDoc(int run) {
            return (int) startDocs.get(run);
        }

        @Override
        public int length(int run) {
            final int end = run + 1 < numRuns ? (int) startDocs.get(run + 1) : maxDoc;
            return end - (int) startDocs.get(run);
        }

        @Override
        public long ordinal(int run) {
            return ordsPerRun.get(run);
        }

        @Override
        public int sentinel() {
            return valueCount;
        }
    }

    private static final class RunTableNumericDocValues extends NumericDocValues {

        private final Cursor cursor;
        private final LongValues ordsPerRun;
        private final int maxDoc;
        private final int sentinel;

        private int doc = -1;

        RunTableNumericDocValues(final Cursor cursor, final LongValues ordsPerRun, int maxDoc, int sentinel) {
            this.cursor = cursor;
            this.ordsPerRun = ordsPerRun;
            this.maxDoc = maxDoc;
            this.sentinel = sentinel;
        }

        @Override
        public long longValue() {
            return ordsPerRun.get(cursor.run());
        }

        @Override
        public boolean advanceExact(int target) {
            cursor.seekDoc(target);
            doc = target;
            return ordsPerRun.get(cursor.run()) != sentinel;
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
            if (ordsPerRun.get(run) != sentinel) {
                doc = target;
                return doc;
            }
            // The target falls in a sentinel (absent) run. Runs are maximal, so the next run carries a real
            // ordinal; skip forward to its first doc. Past the last run there are no more values.
            run++;
            if (run >= cursor.numRuns()) {
                doc = NO_MORE_DOCS;
                return doc;
            }
            cursor.positionOn(run);
            assert ordsPerRun.get(run) != sentinel : "maximal-run invariant broken: two consecutive sentinel runs at run " + run;
            doc = cursor.startDoc(run);
            return doc;
        }

        @Override
        public long cost() {
            return maxDoc;
        }
    }
}
