/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

/**
 * Per-segment histogram values.
 */
public abstract class HistogramValues {

    /**
     * Get the {@link HistogramValue} associated with the current document.
     * The returned {@link HistogramValue} might be reused across calls.
     */
    public abstract HistogramValue histogram() throws IOException;

    /** Advance the iterator to exactly {@code target} and return whether
     *  {@code target} has a value.
     *  {@code target} must be greater than or equal to the current
     *  doc ID and must be a valid doc ID, ie. &ge; 0 and
     *  &lt; {@code maxDoc}.*/
    public abstract boolean advanceExact(int target) throws IOException;

    public int docValueCount() {
        return 1;
    }

    /**
     * @return a doc id iterator over the doc values when available, otherwise null.
     */
    @Nullable
    public DocIdSetIterator docIdIterator() {
        return null;
    }
}
