/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.store.ByteBuffersDataOutput;

import java.io.IOException;
import java.util.List;

/**
 * This is a tree-writer that serializes a list of {@link ShapeField.DecodedTriangle} as an interval tree
 * into a byte array.
 */
public class GeometryDocValueWriter {

    private final TriangleTreeWriter treeWriter;
    private final CoordinateEncoder coordinateEncoder;
    private final CentroidCalculator centroidCalculator;

    public GeometryDocValueWriter(List<ShapeField.DecodedTriangle> triangles, CoordinateEncoder coordinateEncoder,
                                  CentroidCalculator centroidCalculator) {
        this.coordinateEncoder = coordinateEncoder;
        this.centroidCalculator = centroidCalculator;
        this.treeWriter = new TriangleTreeWriter(triangles);
    }

    /*** Serialize the interval tree in the provided data output */
    public void writeTo(ByteBuffersDataOutput out) throws IOException {
        out.writeInt(coordinateEncoder.encodeX(centroidCalculator.getX()));
        out.writeInt(coordinateEncoder.encodeY(centroidCalculator.getY()));
        centroidCalculator.getDimensionalShapeType().writeTo(out);
        out.writeVLong(Double.doubleToLongBits(centroidCalculator.sumWeight()));
        treeWriter.writeTo(out);
    }
}
