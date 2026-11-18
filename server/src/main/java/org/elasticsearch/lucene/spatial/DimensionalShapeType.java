/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.spatial;

import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.ShapeType;

/**
 * Like {@link ShapeType} but has specific
 * types for when the geometry is a {@link GeometryCollection} and
 * more information about what the highest-dimensional sub-shape
 * is.
 *
 * <p>The high bit (0x80) of the serialized byte is used as a format version marker in
 * geometry doc-values. When set, it indicates the new format (V2) with a vertex lookup
 * table and connectivity information for geometry reconstruction. When clear, it indicates
 * the legacy format with inline coordinate deltas.
 */
public enum DimensionalShapeType {
    POINT,
    LINE,
    POLYGON;

    /** Bit mask for the format version marker in the serialized byte. */
    static final int VERSION_BIT = 0x80;

    private static DimensionalShapeType[] values = values();

    public static DimensionalShapeType fromOrdinalByte(byte ordinal) {
        return values[Byte.toUnsignedInt(ordinal) & ~VERSION_BIT];
    }

    /** Returns true if the serialized byte indicates the V2 format (vertex table + connectivity). */
    public static boolean isV2Format(byte rawByte) {
        return (Byte.toUnsignedInt(rawByte) & VERSION_BIT) != 0;
    }

    /** Writes this type in legacy (V1) format without the version bit. */
    public void writeTo(BytesStreamOutput out) {
        out.writeByte((byte) ordinal());
    }

    /** Writes this type in V2 format with the version bit set. */
    public void writeV2To(BytesStreamOutput out) {
        out.writeByte((byte) (ordinal() | VERSION_BIT));
    }

    /**
     * Reads the raw byte and returns the DimensionalShapeType (stripping the version bit).
     * Use {@link #isV2Format(byte)} on the raw byte to determine the format version.
     */
    public static DimensionalShapeType readFrom(ByteArrayStreamInput in) {
        return fromOrdinalByte(in.readByte());
    }
}
