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
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.IndexShapeFieldData;
import org.elasticsearch.xpack.spatial.index.fielddata.ShapeValues;

import java.io.IOException;
import java.util.function.Function;

public abstract class GeoShapeValuesSource extends ShapeValuesSource {
    public static final GeoShapeValuesSource EMPTY = new GeoShapeValuesSource() {

        @Override
        public GeoShapeValues shapeValues(LeafReaderContext context) {
            return GeoShapeValues.EMPTY;
        }
    };

    @Override
    protected Function<Rounding, Rounding.Prepared> roundingPreparer() throws IOException {
        throw new AggregationExecutionException("can't round a [geo_shape]");
    }

    public static class Fielddata extends GeoShapeValuesSource {

        protected final IndexShapeFieldData indexFieldData;

        public Fielddata(IndexShapeFieldData indexFieldData) {
            this.indexFieldData = indexFieldData;
        }

        @Override
        public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
            return indexFieldData.load(context).getBytesValues();
        }

        public ShapeValues shapeValues(LeafReaderContext context) {
            return indexFieldData.load(context).getShapeValues();
        }
    }
}
