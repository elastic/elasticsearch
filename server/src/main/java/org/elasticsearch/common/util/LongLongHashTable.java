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
import org.elasticsearch.core.Releasable;

/**
 * A hash table mapping keys (pair of primitive longs) to values to ids, also
 * of primitive long type. The id's start at 0, and increment monotonically as
 * unique keys are added.
 */
public interface LongLongHashTable extends Releasable, Accountable {

    /** Gets the first key from the given id value. */
    long getKey1(long id);

    /** Gets the second key from the given id value. */
    long getKey2(long id);

    /**
     * Finds the id associated with the given key pair, or -1 is the key
     * pair is not contained in the hash.
     */
    long find(long key1, long key2);

    /**
     * Adds the given key pair to the table. Return its newly allocated id
     * if it wasn't in the table yet, or {@code -1-id} if it was already
     * present in the table.
     */
    long add(long key1, long key2);

    /** Returns the size (number of key/value pairs) in the table.*/
    long size();

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
    default void bulkAdd(long[] key1s, long[] key2s, int[] ids, int length) {
        throw new UnsupportedOperationException("bulkAdd is not supported");
    }
}
