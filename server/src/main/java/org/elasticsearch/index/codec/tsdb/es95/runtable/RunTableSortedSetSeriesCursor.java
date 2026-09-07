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
import org.elasticsearch.index.codec.tsdb.SortedSetRunView;

import java.io.IOException;

/**
 * Per-segment {@link SortedSetSeriesCursor} for a {@code SortedSet} field during merge. It advances a
 * {@link SeriesIterator} over the segment's series and, for each series, reports the global {@code _tsid} ordinal
 * and the ordinal set read from the field's run-table {@link SortedSetRunView}.
 *
 * <p>A field run can span several series when adjacent series share a set, so the field-run pointer co-advances
 * with the series pointer rather than assuming a one-to-one mapping. Each ordinal is remapped to the merged terms
 * dictionary through {@code fieldRemap}; the remap preserves order, so the set stays ascending. An absent series
 * is the empty set and needs no sentinel.
 */
public final class RunTableSortedSetSeriesCursor implements SortedSetSeriesCursor {

    private final SeriesIterator series;
    private final SortedSetRunView fieldRuns;
    private final LongValues fieldRemap;

    private int fieldRun = 0;

    public RunTableSortedSetSeriesCursor(final SeriesIterator series, final SortedSetRunView fieldRuns, final LongValues fieldRemap) {
        this.series = series;
        this.fieldRuns = fieldRuns;
        this.fieldRemap = fieldRemap;
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
    public int ordCount() {
        return fieldRuns.ordCount(fieldRun);
    }

    @Override
    public int ordAt(int index) {
        return (int) fieldRemap.get(fieldRuns.ordAt(fieldRun, index));
    }
}
