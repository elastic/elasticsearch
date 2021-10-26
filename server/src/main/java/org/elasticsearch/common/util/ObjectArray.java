/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

/**
 * Abstraction of an array of object values.
 */
public interface ObjectArray<T> extends BigArray {

    /**
     * Get an element given its index.
     */
    T get(long index);

    /**
     * Set a value at the given index and return the previous value.
     */
    T set(long index, T value);

}
