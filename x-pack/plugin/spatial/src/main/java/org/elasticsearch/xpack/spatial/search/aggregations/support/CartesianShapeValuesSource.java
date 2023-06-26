/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.support;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xpack.spatial.index.fielddata.CartesianShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.IndexShapeFieldData;

import java.util.function.Function;

public abstract class CartesianShapeValuesSource extends ShapeValuesSource<CartesianShapeValues> {
    public static final CartesianShapeValuesSource EMPTY = new CartesianShapeValuesSource() {

        @Override
        public CartesianShapeValues shapeValues(LeafReaderContext context) {
            return CartesianShapeValues.EMPTY;
        }
    };

    @Override
    protected Function<Rounding, Rounding.Prepared> roundingPreparer(AggregationContext context) {
        throw new AggregationExecutionException("can't round a [shape]");
    }

    public static class Fielddata extends CartesianShapeValuesSource {

        protected final IndexShapeFieldData<CartesianShapeValues> indexFieldData;

        public Fielddata(IndexShapeFieldData<CartesianShapeValues> indexFieldData) {
            this.indexFieldData = indexFieldData;
        }

        @Override
        public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
            return indexFieldData.load(context).getBytesValues();
        }

        public CartesianShapeValues shapeValues(LeafReaderContext context) {
            return indexFieldData.load(context).getShapeValues();
        }
    }
}
