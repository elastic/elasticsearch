/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

/**
 * Column types for the binary columnar document batch format.
 */
public enum ColumnType {
    INT((byte) 0, 4),
    LONG((byte) 1, 8),
    STRING((byte) 2, -1),    // variable length
    BOOLEAN((byte) 3, 1),
    BINARY((byte) 4, -1),    // variable length (raw XContent bytes)
    NULL((byte) 5, 0);       // all values are null

    private final byte id;
    private final int fixedSize; // -1 for variable length

    ColumnType(byte id, int fixedSize) {
        this.id = id;
        this.fixedSize = fixedSize;
    }

    public byte id() {
        return id;
    }

    public int fixedSize() {
        return fixedSize;
    }

    public boolean isFixedSize() {
        return fixedSize >= 0;
    }

    public static ColumnType fromId(byte id) {
        return switch (id) {
            case 0 -> INT;
            case 1 -> LONG;
            case 2 -> STRING;
            case 3 -> BOOLEAN;
            case 4 -> BINARY;
            case 5 -> NULL;
            default -> throw new IllegalArgumentException("Unknown column type id: " + id);
        };
    }

    /**
     * Returns the widened type when two columns with different types need to be merged.
     */
    public ColumnType widenWith(ColumnType other) {
        if (this == other) return this;
        if (this == NULL) return other;
        if (other == NULL) return this;
        // INT + LONG -> LONG
        if ((this == INT && other == LONG) || (this == LONG && other == INT)) return LONG;
        // INT/LONG + DOUBLE (stored as LONG bits) -> LONG
        // Note: doubles are stored as LONG via Double.doubleToRawLongBits()
        // Any numeric + STRING -> STRING
        if (this == STRING || other == STRING) return STRING;
        // Scalar + BINARY -> BINARY
        if (this == BINARY || other == BINARY) return BINARY;
        // Any remaining combinations -> STRING as safe fallback
        return STRING;
    }
}
