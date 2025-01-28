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
 * Base implementation for {@link BytesRefHash} and {@link LongHash}, or any class that
 * needs to map values to dense ords. This class is not thread-safe.
 */
// IDs are internally stored as id + 1 so that 0 encodes for an empty slot
abstract class AbstractHash extends AbstractPagedHashMap {

    LongArray ids;

    AbstractHash(long capacity, float maxLoadFactor, BigArrays bigArrays) {
        super(capacity, maxLoadFactor, bigArrays);
        ids = bigArrays.newLongArray(capacity(), true);
    }

    /**
     * Get the id associated with key at <code>0 &lt;= index &lt;= capacity()</code> or -1 if this slot is unused.
     */
    public long id(long index) {
        return ids.get(index) - 1;
    }

    /**
     * Set the id provided key at <code>0 &lt;= index &lt;= capacity()</code> .
     */
    protected final void setId(long index, long id) {
        ids.set(index, id + 1);
    }

    /**
     * Set the id provided key at <code>0 &lt;= index &lt;= capacity()</code>  and get the previous value or -1 if this slot is unused.
     */
    protected final long getAndSetId(long index, long id) {
        return ids.getAndSet(index, id + 1) - 1;
    }

    @Override
    protected void resize(long capacity) {
        ids = bigArrays.resize(ids, capacity);
    }

    @Override
    protected boolean used(long bucket) {
        return id(bucket) >= 0;
    }

    @Override
    public void close() {
        Releasables.close(ids);
    }
}
