/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.core.Releasable;

/**
 * A hash table mapping keys (primitive long type) to values to ids, also of
 * primitive long type. The id's start at 0, and increment monotonically as
 * unique keys are added.
 */
public interface LongHashTable extends Releasable {

    /** Gets a key from the given id value. */
    long get(long id);

    /**
     * Finds the id associated with the given key, or -1 is the key is
     * not contained in the hash.
     */
    long find(long key);

    /**
     * Adds the given key to the table. Return its newly allocated id if
     * it wasn't in the table yet, or {@code -1-id} if it was already
     * present in the table.
     */
    long add(long key);

    /** Returns the size (number of key/value pairs) in the table.*/
    long size();
}
