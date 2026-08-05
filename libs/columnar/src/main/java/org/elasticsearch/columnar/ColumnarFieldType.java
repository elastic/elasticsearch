/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.index.FieldInfo;

/**
 * The logical type of a ColumNAR column, carried on the binary field via
 * {@link ColumNARDocValuesFormat#TYPE_ATTRIBUTE}. It selects the column implementation: numeric types
 * ({@code LONG}/{@code DOUBLE}) share the adaptive long column; {@code STRING} is not yet built.
 */
public enum ColumnarFieldType {

    LONG((byte) 0),
    DOUBLE((byte) 1),
    STRING((byte) 2);

    private final byte id;

    ColumnarFieldType(byte id) {
        this.id = id;
    }

    /** The stable on-disk id; frozen once shipped. */
    public byte id() {
        return id;
    }

    /** Whether this is a numeric type ({@code LONG} or {@code DOUBLE}). */
    public boolean isNumeric() {
        return this == LONG || this == DOUBLE;
    }

    public static ColumnarFieldType fromId(byte id) {
        for (ColumnarFieldType type : values()) {
            if (type.id == id) {
                return type;
            }
        }
        throw new IllegalArgumentException("unknown ColumNAR field type id [" + id + "]");
    }

    /**
     * The type declared on {@code field} via {@link ColumNARDocValuesFormat#TYPE_ATTRIBUTE}. Throws
     * when the attribute is absent (this format only handles fields it was told the type of) or names
     * an unknown type.
     */
    public static ColumnarFieldType fromField(FieldInfo field) {
        String value = field.getAttribute(ColumNARDocValuesFormat.TYPE_ATTRIBUTE);
        if (value == null) {
            throw new IllegalArgumentException(
                "field [" + field.name + "] has no [" + ColumNARDocValuesFormat.TYPE_ATTRIBUTE + "] attribute"
            );
        }
        return valueOf(value);
    }
}
