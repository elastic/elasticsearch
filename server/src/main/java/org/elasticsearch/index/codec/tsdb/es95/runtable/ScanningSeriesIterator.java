/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95.runtable;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.LongValues;

import java.io.IOException;

/**
 * {@link SeriesIterator} that streams series directly from a segment's dense {@code _tsid} sorted doc values (the
 * primary sort field). Because the segment is sorted by {@code _tsid}, equal ordinals are contiguous, so each
 * {@link #next()} scans forward from the previous series to the next {@code _tsid} change, reporting that run's
 * first doc and doc count. The local {@code _tsid} ordinal is remapped to a global ordinal through
 * {@code tsidGlobalOrds} so a k-way merge can order series across segments.
 *
 * <p>The scan is lazy and holds no per-series buffers, so a merge pays the {@code O(docs)} {@code _tsid} walk
 * once per segment without materializing the whole series list.
 */
public final class ScanningSeriesIterator implements SeriesIterator {

    private final SortedDocValues tsid;
    private final LongValues tsidGlobalOrds;
    private final int maxDoc;

    private int nextDoc = 0;
    private long currentTsidOrd;
    private int currentStartDoc;
    private int currentDocCount;

    public ScanningSeriesIterator(final SortedDocValues tsid, final LongValues tsidGlobalOrds, int maxDoc) {
        this.tsid = tsid;
        this.tsidGlobalOrds = tsidGlobalOrds;
        this.maxDoc = maxDoc;
    }

    @Override
    public boolean next() throws IOException {
        if (nextDoc >= maxDoc) {
            return false;
        }
        currentStartDoc = nextDoc;
        final boolean present = tsid.advanceExact(nextDoc);
        assert present : "_tsid must be dense but doc " + nextDoc + " has no value";
        final int localOrd = tsid.ordValue();
        currentTsidOrd = tsidGlobalOrds.get(localOrd);
        int doc = nextDoc + 1;
        while (doc < maxDoc) {
            final boolean docPresent = tsid.advanceExact(doc);
            assert docPresent : "_tsid must be dense but doc " + doc + " has no value";
            if (tsid.ordValue() != localOrd) {
                break;
            }
            doc++;
        }
        currentDocCount = doc - currentStartDoc;
        nextDoc = doc;
        return true;
    }

    @Override
    public long tsidOrd() {
        return currentTsidOrd;
    }

    @Override
    public int startDoc() {
        return currentStartDoc;
    }

    @Override
    public int docCount() {
        return currentDocCount;
    }
}
