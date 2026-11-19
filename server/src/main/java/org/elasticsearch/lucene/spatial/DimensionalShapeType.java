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
 * <p>The high bit (0x80) of the serialized byte marks geometry doc-values in the V2 layout (vertex
 * lookup table, plus a triangle tree and connectivity for anything but points). Indices created on
 * or after {@link org.elasticsearch.index.IndexVersions#GEOMETRY_DOC_VALUES_V2} set it on every value;
 * older indices and transient encodings (such as query shapes) write the legacy triangle-tree layout
 * with the bit clear. Readers pick the layout from this bit, so the index version does not need to be
 * passed to every place that decodes doc-values.
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

    /** Returns true if the serialized byte is a {@link #POINT} in the V2 format. */
    public static boolean isV2Point(byte rawByte) {
        return Byte.toUnsignedInt(rawByte) == (POINT.ordinal() | VERSION_BIT);
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
