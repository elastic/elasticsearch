/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.eirf.EirfArrayReader;
import org.elasticsearch.eirf.EirfKeyValueReader;
import org.elasticsearch.xcontent.Text;

/**
 * A single row in a {@link SourceBatch}, providing typed random-access to column values by
 * column index.
 *
 * <p>Column indices match the leaf indices of the batch's {@link SourceSchema}: column {@code i}
 * is the leaf at position {@code i}. Columns not present in this row report type
 * {@link org.elasticsearch.eirf.EirfType#ABSENT}.
 */
public interface SourceRow {

    /** The schema describing the columns in this batch. */
    SourceSchema schema();

    /** Returns {@code true} if this row carries no column values (an empty document). */
    boolean isEmpty();

    /**
     * The size of this row in bytes. Used as an inexpensive proxy for the original source size when the
     * row has not been re-serialized to XContent bytes.
     */
    int sizeInBytes();

    /** The raw type byte for column {@code col}. */
    byte getTypeByte(int col);

    /** Returns {@code true} if column {@code col} is absent (not present in this row). */
    boolean isAbsent(int col);

    /** Returns {@code true} if column {@code col} has an explicit {@code null} value. */
    boolean isNull(int col);

    boolean getBooleanValue(int col);

    int getIntValue(int col);

    float getFloatValue(int col);

    long getLongValue(int col);

    double getDoubleValue(int col);

    Text getStringValue(int col);

    BytesRef getBinaryValue(int col);

    EirfKeyValueReader getKeyValue(int col);

    EirfArrayReader getArrayValue(int col);
}
