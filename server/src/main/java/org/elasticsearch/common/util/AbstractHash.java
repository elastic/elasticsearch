/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.core.Releasables;

/**
 * Base implementation for {@link BytesRefHash},{@link LongHash}, {@link LongLongHash} and {@link Int3Hash}
 * or any class that needs to map values to dense ords. This class is not thread-safe.
 */
abstract class AbstractHash extends AbstractPagedHashMap {

    LongArray ids;

    AbstractHash(long capacity, float maxLoadFactor, BigArrays bigArrays) {
        super(capacity, maxLoadFactor, bigArrays);
        ids = bigArrays.newLongArray(capacity(), false);
        ids.fill(0L, capacity(), -1L);
    }

    /**
     * Get the id associated with key at <code>0 &lt;= index &lt;= capacity()</code> or -1 if this slot is unused.
     */
    public final long id(long index) {
        return ids.get(index);
    }

    /**
     * Set the id provided key at <code>0 &lt;= index &lt;= capacity()</code> .
     */
    protected final void setId(long index, long id) {
        ids.set(index, id);
    }

    @Override
    public void close() {
        Releasables.close(ids);
    }

    @Override
    protected final void rehash(long buckets, long newBuckets) {
        // grow and reset all ids to -1
        ids = bigArrays.resize(ids, newBuckets);
        ids.fill(0L, newBuckets, -1L);
        // rehash all elements
        final long size = size();
        for (long i = 0; i < size; ++i) {
            rehash(i);
        }
    }

    /**
     * rehash the id.
     */
    protected abstract void rehash(long id);

}
