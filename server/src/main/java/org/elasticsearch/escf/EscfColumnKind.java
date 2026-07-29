/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.elasticsearch.sourcebatch.SourceValueType;

/**
 * Column kind constants for the Elasticsearch Column Format.
 *
 * <pre>
 * LONG/DOUBLE:   values[docCount * 8]
 * BOOL:          value_bitset
 * STRING/BINARY: offsets[(docCount+1) * 4] | bytes
 * ARRAY:         offsets[(docCount+1) * 4] (per-row element ranges) | child_kind(1) | child_values
 * UNION:         type_vec[docCount] | offsets[(docCount+1) * 4] | dense_values
 * </pre>
 * A validity bitset (LE longs, bit set = present/valid) is prepended to any kind only
 * when at least one document is absent. ARRAY uses a columnar list layout: the {@code offsets}
 * delimit each row's element range within a single dense primitive {@code child} sub-column
 * (never inline arrays).
 */
public final class EscfColumnKind {

    /** All values are {@code long}s (JSON ints and longs upcast to 64-bit). */
    public static final byte LONG = 0x01;

    /** All values are {@code double}s (JSON floats and doubles upcast to 64-bit). */
    public static final byte DOUBLE = 0x02;

    /** All values are booleans, stored as a value bitset (bit set = {@code true}). */
    public static final byte BOOL = 0x03;

    /** All values are UTF-8 strings. */
    public static final byte STRING = 0x04;

    /** All values are raw binary bytes. */
    public static final byte BINARY = 0x05;

    /**
     * All values are arrays of a single fixed primitive element kind, stored in a columnar list layout:
     * a per-row element-range offset vector over a dense primitive child sub-column.
     */
    public static final byte ARRAY = 0x06;

    /**
     * A heterogeneous column: a per-row type vector gives each row's {@link SourceValueType},
     * and a dense value buffer holds the payload. Array and key-value rows are stored as inline EIRF
     * bytes here. Handles any type combination, including explicit null and mixed long/double.
     */
    public static final byte UNION = 0x07;

    private EscfColumnKind() {}

    /** Returns a debug name for the given kind byte. */
    public static String name(byte kind) {
        return switch (kind) {
            case LONG -> "LONG";
            case DOUBLE -> "DOUBLE";
            case BOOL -> "BOOL";
            case STRING -> "STRING";
            case BINARY -> "BINARY";
            case ARRAY -> "ARRAY";
            case UNION -> "UNION";
            default -> "UNKNOWN(0x" + Integer.toHexString(kind & 0xFF) + ")";
        };
    }
}
