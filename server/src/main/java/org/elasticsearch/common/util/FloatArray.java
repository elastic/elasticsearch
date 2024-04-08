/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

/**
 * Abstraction of an array of double values.
 */
public interface FloatArray extends BigArray {

    /**
     * Get an element given its index.
     */
    float get(long index);

    /**
     * Set a value at the given index and return the previous value.
     */
    float set(long index, float value);

    /**
     * Fill slots between <code>fromIndex</code> inclusive to <code>toIndex</code> exclusive with <code>value</code>.
     */
    void fill(long fromIndex, long toIndex, float value);

    /**
     * Bulk set.
     */
    void set(long index, byte[] buf, int offset, int len);
}
