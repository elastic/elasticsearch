/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.support;


import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.MissingValues;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.index.fielddata.IndexPointFieldData;
import org.elasticsearch.xpack.spatial.index.fielddata.MultiPointValues;

import java.io.IOException;
import java.util.function.LongSupplier;

public class PointValuesSourceType implements ValuesSourceType {

    static PointValuesSourceType INSTANCE = new PointValuesSourceType();

    public static PointValuesSourceType instance() {
        return INSTANCE;
    }

    @Override
    public String typeName() {
        return "point";
    }

    @Override
    public ValuesSource getEmpty() {
        return PointValuesSource.EMPTY;
    }

    @Override
    public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
        throw new AggregationExecutionException("value source of type [" + this.typeName() + "] is not supported by scripts");
    }

    @Override
    public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
        if (!(fieldContext.indexFieldData() instanceof IndexPointFieldData)) {
            throw new IllegalArgumentException("Expected point type on field [" + fieldContext.field() +
                "], but got [" + fieldContext.fieldType().typeName() + "]");
        }
        return new PointValuesSource.Fielddata((IndexPointFieldData) fieldContext.indexFieldData());
    }

    @Override
    public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat,
                                       LongSupplier nowSupplier) {
        final CartesianPoint missing = new CartesianPoint();
        missing.resetFromString((String) rawMissing, true);
        return new PointValuesSource() {
            @Override
            public MultiPointValues geoPointValues(LeafReaderContext context) {
                PointValuesSource pointValuesSource = (PointValuesSource) valuesSource;
                MultiPointValues values = pointValuesSource.geoPointValues(context);
                return new MultiPointValues() {

                    private int count;

                    @Override
                    public boolean advanceExact(int doc) throws IOException {
                        if (values.advanceExact(doc)) {
                            count = values.docValueCount();
                        } else {
                            count = 0;
                        }
                        // always return true because we want to return a value even if
                        // the document does not have a value
                        return true;
                    }

                    @Override
                    public int docValueCount() {
                        return count == 0 ? 1 : count;
                    }

                    @Override
                    public CartesianPoint nextValue() throws IOException {
                        if (count > 0) {
                            return values.nextValue();
                        } else {
                            return missing;
                        }
                    }
                };
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return MissingValues.replaceMissing(valuesSource.bytesValues(context), new BytesRef(missing.toString()));
            }
        };
    }
}
