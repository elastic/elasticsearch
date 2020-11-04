/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.ByteBuffersDataOutput;

import java.io.IOException;
import java.util.List;

/**
 * This is a tree-writer that serializes a list of {@link IndexableField} as an interval tree
 * into a byte array.
 */
public class GeometryDocValueWriter {

    private GeometryDocValueWriter() {
        // no instances
    }

    /*** Serialize the interval tree in the provided data output */
    public static ByteBuffersDataOutput write(final List<IndexableField> fields,
                                              final CoordinateEncoder coordinateEncoder,
                                              final CentroidCalculator centroidCalculator) throws IOException {
        final ByteBuffersDataOutput out = new ByteBuffersDataOutput();
        out.writeInt(coordinateEncoder.encodeX(centroidCalculator.getX()));
        out.writeInt(coordinateEncoder.encodeY(centroidCalculator.getY()));
        centroidCalculator.getDimensionalShapeType().writeTo(out);
        out.writeVLong(Double.doubleToLongBits(centroidCalculator.sumWeight()));
        TriangleTreeWriter.writeTo(out, fields);
        return out;
    }
}
