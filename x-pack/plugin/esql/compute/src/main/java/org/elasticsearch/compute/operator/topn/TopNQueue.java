/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

interface TopNQueue extends Accountable, Releasable {
    /** The current number of rows in the queue. */
    int size();

    // A proper 3-ctor ADT would be a better design here, but using nulls is more efficient.

    /**
     * @param evictedRow the row that was evicted from the queue, or {@code null} if no row was evicted.
     * @param spareValuesPreAllocSize the new pre-allocation size for the spare row's values.
     */
    record AddResult(@Nullable Row evictedRow, int spareValuesPreAllocSize) implements Releasable {
        @Override
        public void close() {
            Releasables.close(evictedRow);
        }
    }

    // FIXME(gal, NOCOMMIT) The RowFiller should be part of the queue's state, not passed in
    /** Returns {@code null} if the row wasn't added because the queue was full <b>and</b> the row didn't qualify to be added. */
    @Nullable
    AddResult add(RowFiller rowFiller, int i, Row row, int spareValuesPreAllocSize);

    /** Removes and returns the top row in the queue, or {@code null} if the queue is empty. */
    @Nullable
    Row pop();
}
