/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;

/**
 * A hash table mapping keys (of type BytesRef) to values to ids, of
 * primitive long type. The id's start at 0, and increment monotonically
 * as unique keys are added.
 */
public interface BytesRefHashTable extends Accountable, Releasable {

    /** Gets the key at the given id value. */
    BytesRef get(long id, BytesRef dest);

    /**
     * Finds the id associated with the given key, or -1 is the key is
     * not contained in the hash.
     */
    long find(BytesRef key);

    /**
     * Adds the given key to the table. Return its newly allocated id if
     * it wasn't in the table yet, or {@code -1-id} if it was already
     * present in the table.
     */
    long add(BytesRef key);

    /**
     * Adds the given key to the table, copying the remaining bytes.
     * Return its newly allocated id if it wasn't in the table yet, or {@code -1-id}
     * if it was already present in the table. The cursor is drained (advanced to its end)
     * when a new key is inserted.
     */
    long add(PagedBytesCursor key);

    /** Returns the size (number of key/value pairs) in the table.*/
    long size();

    /** Gets an optional the backing bytes ref array. */
    @Nullable
    BytesRefArray getOptionalBackingBytesRefs();

    /**
     * Whether this table supports {@link #bulkAdd}. Implementations return {@code true} only
     * when the table is large enough that prefetch-based bulk insertion outweighs its overhead.
     */
    default boolean supportBulkAdd() {
        return false;
    }

    /**
     * Adds {@code length} key pairs in bulk, writing each group id into {@code ids}.
     * Ids are always non-negative (unlike {@link #add} which encodes duplicates as {@code -1-id}).
     * Only valid when {@link #supportBulkAdd()} returns {@code true}.
     */
    default void bulkAdd(byte[] keyArray, int keyStartOff, int[] ids, int numKeys) {
        throw new UnsupportedOperationException("bulkAdd is not supported");
    }
}
