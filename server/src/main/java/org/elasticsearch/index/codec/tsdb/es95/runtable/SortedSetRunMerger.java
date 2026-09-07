/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

import org.apache.lucene.util.ArrayUtil;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * K-way merges per-segment {@link SortedSetSeriesCursor}s by global {@code _tsid} ordinal and appends the merged
 * series to a {@link RunTableSortedSetOrdinalWriter} at run granularity.
 *
 * <p>Under the TSDB index sort the merged doc order groups docs by {@code _tsid}, and a dimension's ordinal set
 * is constant per series, so emitting series in {@code _tsid} order and coalescing adjacent series that share a
 * set reproduces exactly the run table a per-doc re-encode of the merged doc stream would produce. A series split
 * across source segments appears as consecutive equal-{@code _tsid} entries carrying the same set, so it coalesces
 * into a single run without any per-doc reconciliation.
 */
public final class SortedSetRunMerger {

    private SortedSetRunMerger() {}

    public static void merge(final List<SortedSetSeriesCursor> cursors, final RunTableSortedSetOrdinalWriter writer) throws IOException {
        final PriorityQueue<SortedSetSeriesCursor> queue = new PriorityQueue<>(
            Math.max(1, cursors.size()),
            Comparator.comparingLong(SortedSetSeriesCursor::tsidOrd)
        );
        for (final SortedSetSeriesCursor cursor : cursors) {
            if (cursor.next()) {
                queue.add(cursor);
            }
        }
        int[] set = new int[16];
        while (queue.isEmpty() == false) {
            final SortedSetSeriesCursor cursor = queue.poll();
            final int count = cursor.ordCount();
            set = ArrayUtil.grow(set, count);
            for (int i = 0; i < count; i++) {
                set[i] = cursor.ordAt(i);
            }
            writer.addRun(set, count, cursor.docCount());
            if (cursor.next()) {
                queue.add(cursor);
            }
        }
    }
}
