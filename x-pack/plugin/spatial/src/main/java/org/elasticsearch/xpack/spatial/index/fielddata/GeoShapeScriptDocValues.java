/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.script.GeometryFieldScript;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.index.mapper.BinaryShapeDocValuesField;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;

import java.io.IOException;
import java.util.List;

/**
 * Similarly to what {@link BinaryShapeDocValuesField} does, it encodes the shapes using the {@link GeometryDocValueWriter}.
 */
public final class GeoShapeScriptDocValues extends GeoShapeValues {
    private final GeometryFieldScript script;
    private final GeoShapeValues.GeoShapeValue geoShapeValue = new GeoShapeValues.GeoShapeValue();
    private final GeoShapeIndexer indexer;

    public GeoShapeScriptDocValues(GeometryFieldScript script, String fieldName) {
        this.script = script;
        indexer = new GeoShapeIndexer(Orientation.CCW, fieldName);
    }

    @Override
    public boolean advanceExact(int docId) {
        script.runForDoc(docId);
        return script.count() != 0;
    }

    @Override
    public ValuesSourceType valuesSourceType() {
        return GeoShapeValuesSourceType.instance();
    }

    @Override
    public GeoShapeValue value() throws IOException {
        final Geometry geometry = script.geometry();
        if (geometry == null) {
            return null;
        }
        final List<IndexableField> fields = indexer.getIndexableFields(geometry);
        final CentroidCalculator centroidCalculator = new CentroidCalculator();
        centroidCalculator.add(geometry);
        geoShapeValue.reset(GeometryDocValueWriter.write(fields, CoordinateEncoder.GEO, centroidCalculator));
        return geoShapeValue;
    }
}
