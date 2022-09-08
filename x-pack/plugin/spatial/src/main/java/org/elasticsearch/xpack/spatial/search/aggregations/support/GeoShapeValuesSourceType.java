/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.support;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.MissingValues;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.IndexShapeFieldData;
import org.elasticsearch.xpack.spatial.index.fielddata.ShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.plain.LatLonShapeIndexFieldData;

import java.io.IOException;

public class GeoShapeValuesSourceType extends ShapeValuesSourceType {

    static GeoShapeValuesSourceType INSTANCE = new GeoShapeValuesSourceType();

    public static GeoShapeValuesSourceType instance() {
        return INSTANCE;
    }

    @Override
    public ValuesSource getEmpty() {
        return GeoShapeValuesSource.EMPTY;
    }

    @Override
    public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script, AggregationContext context) {
        boolean isPoint = fieldContext.indexFieldData() instanceof IndexGeoPointFieldData;
        boolean isShape = fieldContext.indexFieldData() instanceof IndexShapeFieldData;
        if (isPoint == false && isShape == false) {
            throw new IllegalArgumentException(
                "Expected geo_point or geo_shape type on field ["
                    + fieldContext.field()
                    + "], but got ["
                    + fieldContext.fieldType().typeName()
                    + "]"
            );
        }
        if (isPoint) {
            return new ValuesSource.GeoPoint.Fielddata((IndexGeoPointFieldData) fieldContext.indexFieldData());
        }
        return new GeoShapeValuesSource.Fielddata((LatLonShapeIndexFieldData) fieldContext.indexFieldData());
    }

    @Override
    public ValuesSource replaceMissing(
        ValuesSource valuesSource,
        Object rawMissing,
        DocValueFormat docValueFormat,
        AggregationContext context
    ) {
        GeoShapeValuesSource shapeValuesSource = (GeoShapeValuesSource) valuesSource;
        final ShapeValues.ShapeValue missing = GeoShapeValues.EMPTY.missing(rawMissing.toString());
        return new GeoShapeValuesSource() {
            @Override
            public GeoShapeValues shapeValues(LeafReaderContext context) {
                ShapeValues values = shapeValuesSource.shapeValues(context);
                return new GeoShapeValues() {

                    private boolean exists;

                    @Override
                    public boolean advanceExact(int doc) throws IOException {
                        exists = values.advanceExact(doc);
                        // always return true because we want to return a value even if
                        // the document does not have a value
                        return true;
                    }

                    @Override
                    public ValuesSourceType valuesSourceType() {
                        return values.valuesSourceType();
                    }

                    @Override
                    public ShapeValue value() throws IOException {
                        return exists ? values.value() : missing;
                    }

                    @Override
                    public String toString() {
                        return "anon MultiShapeValues of [" + super.toString() + "]";
                    }
                };
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return MissingValues.replaceMissing(valuesSource.bytesValues(context), new BytesRef(missing.toString()));
            }
        };
    }

    @Override
    public String typeName() {
        return "geoshape";
    }
}
