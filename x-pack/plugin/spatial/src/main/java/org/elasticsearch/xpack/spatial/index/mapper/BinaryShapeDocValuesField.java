/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.CustomDocValuesField;
import org.elasticsearch.xpack.spatial.index.fielddata.CentroidCalculator;
import org.elasticsearch.xpack.spatial.index.fielddata.CoordinateEncoder;
import org.elasticsearch.xpack.spatial.index.fielddata.GeometryDocValueWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BinaryShapeDocValuesField extends CustomDocValuesField {

    private final List<IndexableField> fields;
    private final CoordinateEncoder coordinateEncoder;
    private final CentroidCalculator centroidCalculator;

    public BinaryShapeDocValuesField(String name, CoordinateEncoder coordinateEncoder) {
        super(name);
        this.fields = new ArrayList<>();
        this.coordinateEncoder = coordinateEncoder;
        this.centroidCalculator = new CentroidCalculator();
    }

    public void add(List<IndexableField> fields, Geometry geometry) {
        this.fields.addAll(fields);
        this.centroidCalculator.add(geometry);
    }

    @Override
    public BytesRef binaryValue() {
        try {
            return GeometryDocValueWriter.write(fields, coordinateEncoder, centroidCalculator);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to encode shape", e);
        }
    }
}
