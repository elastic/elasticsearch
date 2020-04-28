/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.mapper.CustomDocValuesField;
import org.elasticsearch.xpack.spatial.index.fielddata.CentroidCalculator;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeCoordinateEncoder;
import org.elasticsearch.xpack.spatial.index.fielddata.TriangleTreeWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BinaryGeoShapeDocValuesField extends CustomDocValuesField {

    private final List<ShapeField.DecodedTriangle> triangles;
    private final CentroidCalculator centroidCalculator;

    public BinaryGeoShapeDocValuesField(String name, ShapeField.DecodedTriangle[] triangles, CentroidCalculator centroidCalculator) {
        super(name);
        this.triangles = new ArrayList<>(triangles.length);
        this.centroidCalculator = centroidCalculator;
        this.triangles.addAll(Arrays.asList(triangles));
    }

    public void add(ShapeField.DecodedTriangle[] triangles, CentroidCalculator centroidCalculator) {
        this.triangles.addAll(Arrays.asList(triangles));
        this.centroidCalculator.addFrom(centroidCalculator);
    }

    @Override
    public BytesRef binaryValue() {
        try {
            final TriangleTreeWriter writer = new TriangleTreeWriter(triangles, GeoShapeCoordinateEncoder.INSTANCE, centroidCalculator);
            ByteBuffersDataOutput output = new ByteBuffersDataOutput();
            writer.writeTo(output);
            return new BytesRef(output.toArrayCopy(), 0, Math.toIntExact(output.size()));
        } catch (IOException e) {
            throw new ElasticsearchException("failed to encode shape", e);
        }
    }
}
