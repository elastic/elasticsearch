/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.elasticsearch.eirf.EirfKeyValueReader;

/**
 * A forward-only reader over a single array value of a {@link SourceRow}, independent of the physical
 * layout the batch stores the array in.
 *
 * <p>Call {@link #next()} to advance to each element, then read it with the accessor matching
 * {@link #type()} (an {@link org.elasticsearch.eirf.EirfType} byte). Value accessors are pure reads
 * and do not advance the cursor.
 *
 * <p>The row-major (EIRF) format stores arrays inline and reads them with the inline implementation;
 * a column-major format backs this with its own implementation over the array column's child values.
 */
public interface ArrayReader {

    /** Advances to the next element. Returns {@code false} when all elements have been consumed. */
    boolean next();

    /** The {@link org.elasticsearch.eirf.EirfType} byte of the current element. */
    byte type();

    /** Returns {@code true} if the current element is an explicit {@code null}. */
    boolean isNull();

    boolean booleanValue();

    int intValue();

    float floatValue();

    long longValue();

    double doubleValue();

    String stringValue();

    /** A reader over the current element's payload; the element must be a {@code UNION_ARRAY} or {@code FIXED_ARRAY}. */
    ArrayReader nestedArray();

    /** A reader over the current element's payload; the element must be a {@code KEY_VALUE}. */
    EirfKeyValueReader nestedKeyValue();
}
