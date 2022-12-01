/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

public interface LongDoubleDoubleArray extends BigArray { // TODO: implement Writeable

    /**
     * Get the position-0 long element given its index.
     */
    long getLong0(long index);

    /**
     * Get the position-0 double element given its index.
     */
    double getDouble0(long index);

    /**
     * Get the position-1 double element given its index.
     */
    double getDouble1(long index);

    /**
     * Set a triple-value at the given index.
     * TODO: we could return a MH that allows retrieval of the previous value.
     */
    void set(long index, long lValue0, double dValue0, double dValue1);

    void increment(long index, long lValue0Inc, double dValue0Inc, double dValue1Inc);

    // TODO: considering adding:
    // - fill
    // - bulk byte[] set

}
