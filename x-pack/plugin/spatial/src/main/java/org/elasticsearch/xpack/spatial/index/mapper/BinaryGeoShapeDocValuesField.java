/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.mapper.CustomDocValuesField;
import org.elasticsearch.xpack.spatial.index.fielddata.CentroidCalculator;
import org.elasticsearch.xpack.spatial.index.fielddata.CoordinateEncoder;
import org.elasticsearch.xpack.spatial.index.fielddata.GeometryDocValueWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BinaryGeoShapeDocValuesField extends CustomDocValuesField {

    private final List<IndexableField> fields;
    private final CentroidCalculator centroidCalculator;

    public BinaryGeoShapeDocValuesField(String name, List<IndexableField> fields, CentroidCalculator centroidCalculator) {
        super(name);
        this.fields = new ArrayList<>(fields.size());
        this.centroidCalculator = centroidCalculator;
        this.fields.addAll(fields);
    }

    public void add( List<IndexableField> fields, CentroidCalculator centroidCalculator) {
        this.fields.addAll(fields);
        this.centroidCalculator.addFrom(centroidCalculator);
    }

    @Override
    public BytesRef binaryValue() {
        try {
            return GeometryDocValueWriter.write(fields, CoordinateEncoder.GEO, centroidCalculator);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to encode shape", e);
        }
    }
}
