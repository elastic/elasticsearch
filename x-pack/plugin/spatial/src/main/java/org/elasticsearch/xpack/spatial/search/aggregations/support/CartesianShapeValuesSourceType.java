/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.support;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.MissingValues;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.index.fielddata.CartesianShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.IndexCartesianPointFieldData;
import org.elasticsearch.xpack.spatial.index.fielddata.IndexShapeFieldData;
import org.elasticsearch.xpack.spatial.index.fielddata.plain.CartesianShapeIndexFieldData;

import java.io.IOException;

public class CartesianShapeValuesSourceType extends ShapeValuesSourceType {

    static CartesianShapeValuesSourceType INSTANCE = new CartesianShapeValuesSourceType();

    public static CartesianShapeValuesSourceType instance() {
        return INSTANCE;
    }

    @Override
    public ValuesSource getEmpty() {
        return CartesianShapeValuesSource.EMPTY;
    }

    @Override
    public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
        boolean isPoint = fieldContext.indexFieldData() instanceof IndexCartesianPointFieldData;
        boolean isShape = fieldContext.indexFieldData() instanceof IndexShapeFieldData;
        if (isPoint == false && isShape == false) {
            throw new IllegalArgumentException(
                "Expected point or shape type on field ["
                    + fieldContext.field()
                    + "], but got ["
                    + fieldContext.fieldType().typeName()
                    + "]"
            );
        }
        if (isPoint) {
            return new CartesianPointValuesSource.Fielddata((IndexCartesianPointFieldData) fieldContext.indexFieldData());
        }
        return new CartesianShapeValuesSource.Fielddata((CartesianShapeIndexFieldData) fieldContext.indexFieldData());
    }

    @Override
    public ValuesSource replaceMissing(
        ValuesSource valuesSource,
        Object rawMissing,
        DocValueFormat docValueFormat,
        AggregationContext context
    ) {
        CartesianShapeValuesSource shapeValuesSource = (CartesianShapeValuesSource) valuesSource;
        final CartesianShapeValues.CartesianShapeValue missing = CartesianShapeValues.EMPTY.missing(rawMissing.toString());
        return new CartesianShapeValuesSource() {
            @Override
            public CartesianShapeValues shapeValues(LeafReaderContext context) {
                CartesianShapeValues values = shapeValuesSource.shapeValues(context);
                return new CartesianShapeValues() {

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
                    public CartesianShapeValue value() throws IOException {
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
        return "shape";
    }
}
