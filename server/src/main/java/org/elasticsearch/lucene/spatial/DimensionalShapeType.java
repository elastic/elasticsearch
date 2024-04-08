/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
 */
public enum DimensionalShapeType {
    POINT,
    LINE,
    POLYGON;

    private static DimensionalShapeType[] values = values();

    public static DimensionalShapeType fromOrdinalByte(byte ordinal) {
        return values[Byte.toUnsignedInt(ordinal)];
    }

    public void writeTo(BytesStreamOutput out) {
        out.writeByte((byte) ordinal());
    }

    public static DimensionalShapeType readFrom(ByteArrayStreamInput in) {
        return fromOrdinalByte(in.readByte());
    }
}
