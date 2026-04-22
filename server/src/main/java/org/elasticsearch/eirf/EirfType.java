/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.eirf;

/**
 * Type byte constants for the Elastic Internal Row Format (EIRF).
 */
public final class EirfType {

    // 0-byte fixed types
    public static final byte NULL = 0x00;
    public static final byte TRUE = 0x01;
    public static final byte FALSE = 0x02;

    // 4-byte scalar types
    public static final byte INT = 0x03;
    public static final byte FLOAT = 0x04;

    // Variable-length types
    public static final byte STRING = 0x05;
    public static final byte BINARY = 0x06;
    public static final byte UNION_ARRAY = 0x07;
    public static final byte FIXED_ARRAY = 0x08;
    public static final byte KEY_VALUE = 0x09;

    // 8-byte scalar types
    public static final byte LONG = 0x0A;
    public static final byte DOUBLE = 0x0B;

    /** Small row threshold: var section must be ≤ 65,535 bytes. */
    public static final int SMALL_ROW_MAX_VAR_SIZE = 65535;

    private EirfType() {}

    /**
     * Fixed-section slot size in bytes for the given type byte and row size.
     *
     * <p>Uses two comparisons: first checks for zero-byte types ({@code < 0x03}),
     * then uses a threshold that depends on the row size to split 4-byte from 8-byte.
     */
    public static int fixedSize(byte typeByte, boolean smallRow) {
        if (typeByte < INT) return 0;
        if (smallRow) {
            return typeByte < LONG ? 4 : 8;
        } else {
            return typeByte < STRING ? 4 : 8;
        }
    }

    /**
     * Returns true if this type has a variable-length payload.
     */
    public static boolean isVariable(byte typeByte) {
        return typeByte >= STRING && typeByte <= KEY_VALUE;
    }

    /**
     * Returns the data size of this type in element position (inside arrays and KEY_VALUE values).
     * Returns -1 for variable-length types (caller must read the i32 length prefix).
     */
    public static int elemDataSize(byte typeByte) {
        return switch (typeByte) {
            case NULL, TRUE, FALSE -> 0;
            case INT, FLOAT -> 4;
            case LONG, DOUBLE -> 8;
            default -> -1; // STRING, KEY_VALUE, UNION_ARRAY, FIXED_ARRAY, BINARY: length-prefixed
        };
    }

    /**
     * Returns true if this type is a compound type (KEY_VALUE or array).
     */
    public static boolean isCompound(byte typeByte) {
        return typeByte == KEY_VALUE || typeByte == UNION_ARRAY || typeByte == FIXED_ARRAY;
    }

    public static String name(byte typeByte) {
        return switch (typeByte) {
            case NULL -> "NULL";
            case TRUE -> "TRUE";
            case FALSE -> "FALSE";
            case INT -> "INT";
            case FLOAT -> "FLOAT";
            case STRING -> "STRING";
            case BINARY -> "BINARY";
            case UNION_ARRAY -> "UNION_ARRAY";
            case FIXED_ARRAY -> "FIXED_ARRAY";
            case KEY_VALUE -> "KEY_VALUE";
            case LONG -> "LONG";
            case DOUBLE -> "DOUBLE";
            default -> "UNKNOWN(0x" + Integer.toHexString(typeByte & 0xFF) + ")";
        };
    }
}
