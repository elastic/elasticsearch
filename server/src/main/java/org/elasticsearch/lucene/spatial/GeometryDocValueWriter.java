/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.lucene.spatial;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;

import java.io.IOException;
import java.util.List;

/**
 * This is a tree-writer that serializes a list of {@link IndexableField} as an interval tree
 * into a byte array.
 */
public class GeometryDocValueWriter {

    private GeometryDocValueWriter() {}

    /*** Serialize the triangle tree in a BytesRef */
    public static BytesRef write(List<IndexableField> fields, CoordinateEncoder coordinateEncoder, CentroidCalculator centroidCalculator)
        throws IOException {
        final BytesStreamOutput out = new BytesStreamOutput();
        // normalization may be required due to floating point precision errors
        out.writeInt(coordinateEncoder.encodeX(coordinateEncoder.normalizeX(centroidCalculator.getX())));
        out.writeInt(coordinateEncoder.encodeY(coordinateEncoder.normalizeY(centroidCalculator.getY())));
        centroidCalculator.getDimensionalShapeType().writeTo(out);
        out.writeVLong(Double.doubleToLongBits(centroidCalculator.sumWeight()));
        TriangleTreeWriter.writeTo(out, fields);
        return out.bytes().toBytesRef();
    }
}
