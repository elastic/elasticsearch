/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

import org.apache.lucene.util.LongValues;
import org.elasticsearch.index.codec.tsdb.SortedRunView;

import java.io.IOException;

/**
 * Per-segment {@link SortedSeriesCursor} for a {@code Sorted} field during merge. It advances a
 * {@link SeriesIterator} over the segment's series and, for each series, reports the global {@code _tsid} ordinal
 * and the field ordinal read from the field's run-table {@link SortedRunView}.
 *
 * <p>A field run can span several series when adjacent series share a value, so the field-run pointer co-advances
 * with the series pointer rather than assuming a one-to-one mapping. The field ordinal is remapped to the merged
 * terms dictionary through {@code fieldRemap}, except an absent series, whose source run carries the source
 * sentinel, is emitted as the merged sentinel: the ordinal map covers only the real ordinals {@code [0, K)}.
 */
public final class RunTableSortedSeriesCursor implements SortedSeriesCursor {

    private final SeriesIterator series;
    private final SortedRunView fieldRuns;
    private final LongValues fieldRemap;
    private final int mergedSentinel;

    private int fieldRun = 0;

    public RunTableSortedSeriesCursor(
        final SeriesIterator series,
        final SortedRunView fieldRuns,
        final LongValues fieldRemap,
        int mergedSentinel
    ) {
        this.series = series;
        this.fieldRuns = fieldRuns;
        this.fieldRemap = fieldRemap;
        this.mergedSentinel = mergedSentinel;
    }

    @Override
    public boolean next() throws IOException {
        if (series.next() == false) {
            return false;
        }
        final int start = series.startDoc();
        while (fieldRun + 1 < fieldRuns.count() && fieldRuns.startDoc(fieldRun + 1) <= start) {
            fieldRun++;
        }
        return true;
    }

    @Override
    public long tsidOrd() {
        return series.tsidOrd();
    }

    @Override
    public int docCount() {
        return series.docCount();
    }

    @Override
    public long fieldOrd() {
        final long sourceOrd = fieldRuns.ordinal(fieldRun);
        return sourceOrd == fieldRuns.sentinel() ? mergedSentinel : fieldRemap.get(sourceOrd);
    }
}
