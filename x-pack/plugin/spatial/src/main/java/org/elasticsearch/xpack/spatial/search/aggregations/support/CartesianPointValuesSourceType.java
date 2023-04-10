/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.support;

import org.apache.lucene.geo.XYEncodingUtils;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.MissingValues;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.index.fielddata.IndexCartesianPointFieldData;

import java.io.IOException;

public class CartesianPointValuesSourceType implements Writeable, ValuesSourceType {

    static CartesianPointValuesSourceType INSTANCE = new CartesianPointValuesSourceType();

    public static CartesianPointValuesSourceType instance() {
        return INSTANCE;
    }

    @Override
    public ValuesSource getEmpty() {
        return CartesianPointValuesSource.EMPTY;
    }

    @Override
    public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
        // TODO (support scripts)
        throw new UnsupportedOperationException("point");
    }

    @Override
    public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
        if (fieldContext.indexFieldData() instanceof IndexCartesianPointFieldData pointFieldData) {
            return new CartesianPointValuesSource.Fielddata(pointFieldData);
        }
        throw new IllegalArgumentException(
            "Expected point type on field [" + fieldContext.field() + "], but got [" + fieldContext.fieldType().typeName() + "]"
        );
    }

    @Override
    public ValuesSource replaceMissing(
        ValuesSource valuesSource,
        Object rawMissing,
        DocValueFormat docValueFormat,
        AggregationContext context
    ) {
        // TODO: also support the structured formats of points
        final CartesianPointValuesSource pointValuesSource = (CartesianPointValuesSource) valuesSource;
        final CartesianPoint missing = new CartesianPoint().resetFromString(rawMissing.toString(), false);
        return new CartesianPointValuesSource() {
            @Override
            public SortedNumericDocValues sortedNumericDocValues(LeafReaderContext context) {
                final long xi = XYEncodingUtils.encode((float) missing.getX());
                final long yi = XYEncodingUtils.encode((float) missing.getY());
                long encoded = (yi & 0xFFFFFFFFL) | xi << 32;
                return MissingValues.replaceMissing(pointValuesSource.sortedNumericDocValues(context), encoded);
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return MissingValues.replaceMissing(pointValuesSource.bytesValues(context), new BytesRef(missing.toString()));
            }

            @Override
            public String toString() {
                return "anon CartesianPointValuesSource of [" + super.toString() + "]";
            }

        };
    }

    @Override
    public String typeName() {
        return "point";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }
}
