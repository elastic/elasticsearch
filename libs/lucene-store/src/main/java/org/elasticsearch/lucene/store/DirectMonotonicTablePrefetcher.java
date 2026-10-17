/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.store;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;

import java.io.IOException;

/**
 * Prefetches a {@code DirectMonotonic} table: whole when it lies within a single read unit, otherwise an entry at a time.
 */
public final class DirectMonotonicTablePrefetcher {

    private final PrefetchRanges prefetches;
    private final IndexInput data;
    private final long offset;
    private final long length;
    private final DirectMonotonicReader.Meta meta;
    private final DirectMonotonicLocator locator;
    private boolean askedWhole;
    private LongValues values;

    public DirectMonotonicTablePrefetcher(
        PrefetchRanges prefetches,
        IndexInput data,
        long offset,
        long length,
        DirectMonotonicReader.Meta meta,
        DirectMonotonicLocator locator
    ) {
        this.prefetches = prefetches;
        this.data = prefetches.wrap(data.clone());
        this.offset = offset;
        this.length = length;
        this.meta = meta;
        this.locator = locator;
    }

    /** Asks for the whole table if it lies within a single read unit. */
    public boolean askWholeIfWithinOneUnit() throws IOException {
        if (askedWhole || prefetches.withinOneUnit(offset, length) == false) {
            return false;
        }
        return askWhole();
    }

    /** Asks for the whole table. */
    public boolean askWhole() throws IOException {
        if (askedWhole) {
            return false;
        }
        askedWhole = true;
        data.prefetch(offset, length);
        return true;
    }

    /** Asks for entry {@code index}, unless the whole table was. */
    public boolean askEntry(long index) throws IOException {
        return askedWhole == false && locator.prefetch(data, offset, index);
    }

    /** The table's entries. */
    public LongValues values() throws IOException {
        if (values == null) {
            values = DirectMonotonicReader.getInstance(meta, data.randomAccessSlice(offset, length));
        }
        return values;
    }
}
