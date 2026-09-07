/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * K-way merges per-segment {@link SortedSeriesCursor}s by global {@code _tsid} ordinal and appends the merged
 * series to a {@link RunTableSortedOrdinalWriter} at run granularity.
 *
 * <p>Under the TSDB index sort the merged doc order groups docs by {@code _tsid}, and a dimension ordinal is
 * constant per series, so emitting series in {@code _tsid} order and coalescing adjacent series that share a
 * field ordinal reproduces exactly the run table a per-doc re-encode of the merged doc stream would produce. A
 * series split across source segments appears as consecutive equal-{@code _tsid} entries carrying the same field
 * ordinal, so it coalesces into a single run without any per-doc reconciliation.
 */
public final class SortedRunMerger {

    private SortedRunMerger() {}

    public static void merge(final List<SortedSeriesCursor> cursors, final RunTableSortedOrdinalWriter writer) throws IOException {
        final PriorityQueue<SortedSeriesCursor> queue = new PriorityQueue<>(
            Math.max(1, cursors.size()),
            Comparator.comparingLong(SortedSeriesCursor::tsidOrd)
        );
        for (final SortedSeriesCursor cursor : cursors) {
            if (cursor.next()) {
                queue.add(cursor);
            }
        }
        long previousTsidOrd = -1;
        long previousFieldOrd = -1;
        while (queue.isEmpty() == false) {
            final SortedSeriesCursor cursor = queue.poll();
            final long tsidOrd = cursor.tsidOrd();
            final long fieldOrd = cursor.fieldOrd();
            assert tsidOrd != previousTsidOrd || fieldOrd == previousFieldOrd
                : "same _tsid ordinal " + tsidOrd + " carries different field ordinals " + previousFieldOrd + " and " + fieldOrd;
            writer.addRun((int) fieldOrd, cursor.docCount());
            previousTsidOrd = tsidOrd;
            previousFieldOrd = fieldOrd;
            if (cursor.next()) {
                queue.add(cursor);
            }
        }
    }
}
