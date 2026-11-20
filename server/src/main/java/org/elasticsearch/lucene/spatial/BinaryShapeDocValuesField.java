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
    private final boolean supportsReconstruction;
    private final List<Geometry> geometries;

    /**
     * @param format the format to write. Field mappers write the format of their index version
     *               ({@link GeometryDocValueFormat#forIndexVersion}). {@link GeometryDocValueFormat#LEGACY} is
     *               also cheaper for transient encodings that only need the triangle tree.
     */
    public BinaryShapeDocValuesField(String name, CoordinateEncoder coordinateEncoder, GeometryDocValueFormat format) {
        super(name);
        this.fields = new ArrayList<>();
        this.coordinateEncoder = coordinateEncoder;
        this.centroidCalculator = new CentroidCalculator();
        this.supportsReconstruction = format == GeometryDocValueFormat.V2;
        this.geometries = new ArrayList<>();
    }

    /**
     * Add tessellated fields and the original geometry. The original (pre-normalization) geometry
     * is used for both centroid calculation and connectivity reconstruction. Using the original
     * rather than the normalized geometry produces doc-values that are closer to the source,
     * differing only in coordinate quantization.
     *
     * @param fields   tessellated triangle fields from the indexer
     * @param geometry the original geometry
     */
    public void add(List<IndexableField> fields, Geometry geometry) {
        this.fields.addAll(fields);
        this.centroidCalculator.add(geometry);
        if (supportsReconstruction) {
            this.geometries.add(geometry);
        }
    }

    @Override
    public BytesRef binaryValue() {
        try {
            return supportsReconstruction
                ? GeometryDocValueWriter.write(fields, coordinateEncoder, centroidCalculator, geometries)
                : GeometryDocValueWriter.writeLegacy(fields, coordinateEncoder, centroidCalculator);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to encode shape", e);
        }
    }
}
