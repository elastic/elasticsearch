/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.support;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.index.fielddata.DocValueBits;
import org.elasticsearch.index.fielddata.MultiPointValues;
import org.elasticsearch.index.fielddata.PointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.index.fielddata.IndexCartesianPointFieldData;

import java.io.IOException;
import java.util.function.Function;

public abstract class CartesianPointValuesSource extends ValuesSource {

    public static final CartesianPointValuesSource EMPTY = new CartesianPointValuesSource() {

        @Override
        public SortedNumericDocValues sortedNumericDocValues(LeafReaderContext context) {
            return DocValues.emptySortedNumeric();
        }

        @Override
        public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
            return org.elasticsearch.index.fielddata.FieldData.emptySortedBinary();
        }

    };

    @Override
    public DocValueBits docsWithValue(LeafReaderContext context) {
        final MultiCartesianPointValues pointValues = pointValues(context);
        return org.elasticsearch.index.fielddata.FieldData.docsWithValue(pointValues);
    }

    @Override
    public final Function<Rounding, Rounding.Prepared> roundingPreparer(AggregationContext context) {
        throw new AggregationExecutionException("can't round a [POINT]");
    }

    /**
     * Return point values.
     */
    public final MultiCartesianPointValues pointValues(LeafReaderContext context) {
        return new MultiCartesianPointValues(sortedNumericDocValues(context));
    }

    public static final class MultiCartesianPointValues extends MultiPointValues<CartesianPoint> {
        private final CartesianPoint point = new CartesianPoint();

        public MultiCartesianPointValues(SortedNumericDocValues numericValues) {
            super(numericValues);
        }

        @Override
        public CartesianPoint nextValue() throws IOException {
            return point.resetFromEncoded(numericValues.nextValue());
        }

        @Override
        protected PointValues<CartesianPoint> getPointValues() {
            final NumericDocValues singleton = DocValues.unwrapSingleton(numericValues);
            return singleton != null ? new CartesianPointValues(singleton) : null;
        }
    }

    public static final class CartesianPointValues extends PointValues<CartesianPoint> {

        private final CartesianPoint point = new CartesianPoint();

        CartesianPointValues(NumericDocValues values) {
            super(values);
        }

        @Override
        public CartesianPoint pointValue() throws IOException {
            return point.resetFromEncoded(values.longValue());
        }
    }

    public static final class CartesianPointValue implements ToXContentFragment {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return null;
        }
    }

    /**
     * Return the internal representation of point doc values as a {@link SortedNumericDocValues}.
     */
    public abstract SortedNumericDocValues sortedNumericDocValues(LeafReaderContext context);

    public static class Fielddata extends CartesianPointValuesSource {

        protected final IndexCartesianPointFieldData indexFieldData;

        public Fielddata(IndexCartesianPointFieldData indexFieldData) {
            this.indexFieldData = indexFieldData;
        }

        @Override
        public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
            return indexFieldData.load(context).getBytesValues();
        }

        @Override
        public SortedNumericDocValues sortedNumericDocValues(LeafReaderContext context) {
            return indexFieldData.load(context).getSortedNumericDocValues();
        }
    }
}
