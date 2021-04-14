/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.support;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.index.fielddata.DocValueBits;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.xpack.spatial.index.fielddata.IndexGeoShapeFieldData;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;

import java.io.IOException;
import java.util.function.Function;

public abstract class GeoShapeValuesSource extends ValuesSource {
    public static final GeoShapeValuesSource EMPTY = new GeoShapeValuesSource() {

        @Override
        public GeoShapeValues geoShapeValues(LeafReaderContext context) {
            return GeoShapeValues.EMPTY;
        }

        @Override
        public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
            return FieldData.emptySortedBinary();
        }

    };

    public abstract GeoShapeValues geoShapeValues(LeafReaderContext context);

    @Override
    protected Function<Rounding, Rounding.Prepared> roundingPreparer() throws IOException {
        throw new AggregationExecutionException("can't round a [geo_shape]");
    }

    @Override
    public DocValueBits docsWithValue(LeafReaderContext context) throws IOException {
        GeoShapeValues values = geoShapeValues(context);
        return new DocValueBits() {
            @Override
            public boolean advanceExact(int doc) throws IOException {
                return values.advanceExact(doc);
            }
        };
    }

    public static class Fielddata extends GeoShapeValuesSource {

        protected final IndexGeoShapeFieldData indexFieldData;

        public Fielddata(IndexGeoShapeFieldData indexFieldData) {
            this.indexFieldData = indexFieldData;
        }

        @Override
        public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
            return indexFieldData.load(context).getBytesValues();
        }

        public GeoShapeValues geoShapeValues(LeafReaderContext context) {
            return indexFieldData.load(context).getGeoShapeValues();
        }
    }
}
