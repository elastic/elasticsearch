/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.List;

/**
 * This is a tree-writer that serializes a list of {@link IndexableField} as an interval tree
 * into a byte array.
 */
public class GeometryDocValueWriter {

    private GeometryDocValueWriter() {
    }

    /*** Serialize the triangle tree in a BytesRef */
    public static BytesRef write(List<IndexableField> fields,
                                 CoordinateEncoder coordinateEncoder,
                                 CentroidCalculator centroidCalculator) throws IOException {
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        // normalization may be required due to floating point precision errors
        CodecUtil.writeBEInt(out, coordinateEncoder.encodeX(coordinateEncoder.normalizeX(centroidCalculator.getX())));
        CodecUtil.writeBEInt(out, coordinateEncoder.encodeY(coordinateEncoder.normalizeY(centroidCalculator.getY())));
        centroidCalculator.getDimensionalShapeType().writeTo(out);
        out.writeVLong(Double.doubleToLongBits(centroidCalculator.sumWeight()));
        TriangleTreeWriter.writeTo(out, fields);
        return new BytesRef(out.toArrayCopy(), 0, Math.toIntExact(out.size()));
    }
}
