/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.support;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.index.fielddata.DocValueBits;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.xpack.spatial.index.fielddata.IndexPointFieldData;
import org.elasticsearch.xpack.spatial.index.fielddata.MultiPointValues;

import java.io.IOException;
import java.util.function.Function;

public abstract class PointValuesSource extends ValuesSource {

    public static final PointValuesSource EMPTY = new PointValuesSource() {

        @Override
        public MultiPointValues geoPointValues(LeafReaderContext context) {
            return MultiPointValues.EMPTY;
        }

        @Override
        public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
            return org.elasticsearch.index.fielddata.FieldData.emptySortedBinary();
        }

    };

    @Override
    public DocValueBits docsWithValue(LeafReaderContext context) throws IOException {
        final MultiPointValues values = geoPointValues(context);
        return new DocValueBits() {
            @Override
            public boolean advanceExact(int doc) throws IOException {
                return values.advanceExact(doc);
            }
        };
    }

    @Override
    public final Function<Rounding, Rounding.Prepared> roundingPreparer(IndexReader reader) throws IOException {
        throw new AggregationExecutionException("can't round a [POINT]");
    }

    public abstract MultiPointValues geoPointValues(LeafReaderContext context);

    public static class Fielddata extends PointValuesSource {

        protected final IndexPointFieldData indexFieldData;

        public Fielddata(IndexPointFieldData indexFieldData) {
            this.indexFieldData = indexFieldData;
        }

        @Override
        public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
            return indexFieldData.load(context).getBytesValues();
        }

        public MultiPointValues geoPointValues(LeafReaderContext context) {
            return indexFieldData.load(context).getPointValues();
        }
    }
}
