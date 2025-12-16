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

    /** Finds the id from the given key value. */
    long find(long key);

    /**
     * Adds the given key to the table. Returns -1 if the key is already
     * present, otherwise returns the next id.
     */
    long add(long key);

    /** Returns the size (number of key/value pairs) in the table.*/
    long size();
}
