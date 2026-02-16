/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.spatial;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.CustomDocValuesField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BinaryShapeDocValuesField extends CustomDocValuesField {

    private final List<IndexableField> fields;
    private final CoordinateEncoder coordinateEncoder;
    private final CentroidCalculator centroidCalculator;
    private final List<Geometry> normalizedGeometries;

    public BinaryShapeDocValuesField(String name, CoordinateEncoder coordinateEncoder) {
        super(name);
        this.fields = new ArrayList<>();
        this.coordinateEncoder = coordinateEncoder;
        this.centroidCalculator = new CentroidCalculator();
        this.normalizedGeometries = new ArrayList<>();
    }

    /**
     * Add tessellated fields and both the original and normalized geometry.
     * The original geometry is used for centroid calculation (better precision for edge cases
     * like coordinates > 180 longitude). The normalized geometry is used for connectivity
     * (its vertex ordering after dateline normalization is what gets stored in doc-values).
     *
     * @param fields              tessellated triangle fields from the indexer
     * @param originalGeometry    the original geometry for centroid calculation
     * @param normalizedGeometry  the normalized geometry for connectivity/reconstruction
     */
    public void add(List<IndexableField> fields, Geometry originalGeometry, Geometry normalizedGeometry) {
        this.fields.addAll(fields);
        this.centroidCalculator.add(originalGeometry);
        this.normalizedGeometries.add(normalizedGeometry);
    }

    @Override
    public BytesRef binaryValue() {
        try {
            return GeometryDocValueWriter.write(fields, coordinateEncoder, centroidCalculator, normalizedGeometries);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to encode shape", e);
        }
    }
}
