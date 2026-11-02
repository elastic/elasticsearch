/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

/**
 * The logical type of a ColumNAR column, resolved by an injected {@link ColumnarFieldTypeSelector} when a
 * field is written and re-read from the column metadata at read time. It selects the column implementation: numeric types
 * ({@code LONG}/{@code DOUBLE}) share the adaptive long column, and {@code STRING} uses the string column. How
 * a column encodes its values within that choice — a numeric pipeline, or the layout a string column was
 * written with — is internal to the column and recorded in its own metadata.
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
}
