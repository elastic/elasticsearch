/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.aggregations.support;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.Rounding.Prepared;
import org.elasticsearch.index.fielddata.DocValueBits;
import org.elasticsearch.index.fielddata.HistogramValues;
import org.elasticsearch.index.fielddata.IndexHistogramFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.function.Function;

public class HistogramValuesSource {
    public abstract static class Histogram extends org.elasticsearch.search.aggregations.support.ValuesSource {

        public abstract HistogramValues getHistogramValues(LeafReaderContext context) throws IOException;

        @Override
        public Function<Rounding, Prepared> roundingPreparer(AggregationContext context) throws IOException {
            throw new AggregationExecutionException("can't round a [histogram]");
        }

        public static class Fielddata extends Histogram {

            protected final IndexHistogramFieldData indexFieldData;

            public Fielddata(IndexHistogramFieldData indexFieldData) {
                this.indexFieldData = indexFieldData;
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
                return indexFieldData.load(context).getBytesValues();
            }

            @Override
            public DocValueBits docsWithValue(LeafReaderContext context) throws IOException {
                HistogramValues values = getHistogramValues(context);
                return new DocValueBits() {
                    @Override
                    public boolean advanceExact(int doc) throws IOException {
                        return values.advanceExact(doc);
                    }
                };
            }

            @Override
            public HistogramValues getHistogramValues(LeafReaderContext context) throws IOException {
                return indexFieldData.load(context).getHistogramValues();
            }
        }
    }
}
