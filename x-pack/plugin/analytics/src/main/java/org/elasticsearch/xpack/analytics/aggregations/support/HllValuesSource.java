/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.aggregations.support;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.Rounding.Prepared;
import org.elasticsearch.index.fielddata.DocValueBits;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.xpack.analytics.mapper.fielddata.HllValues;
import org.elasticsearch.xpack.analytics.mapper.fielddata.IndexHllFieldData;

import java.io.IOException;
import java.util.function.Function;

public class HllValuesSource {
    public abstract static class HllSketch extends org.elasticsearch.search.aggregations.support.ValuesSource {

        public abstract HllValues getHllValues(LeafReaderContext context) throws IOException;

        @Override
        public Function<Rounding, Prepared> roundingPreparer(IndexReader reader) throws IOException {
            throw new AggregationExecutionException("can't round a [cardinality]");
        }

        public static class Fielddata extends HllSketch {

            protected final IndexHllFieldData indexFieldData;

            public Fielddata(IndexHllFieldData indexFieldData) {
                this.indexFieldData = indexFieldData;
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
                return indexFieldData.load(context).getBytesValues();
            }

            @Override
            public DocValueBits docsWithValue(LeafReaderContext context) throws IOException {
                HllValues values = getHllValues(context);
                return new DocValueBits() {
                    @Override
                    public boolean advanceExact(int doc) throws IOException {
                        return values.advanceExact(doc);
                    }
                };
            }

            @Override
            public HllValues getHllValues(LeafReaderContext context) throws IOException {
                return indexFieldData.load(context).getHllValues();
            }
        }
    }
}
