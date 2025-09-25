/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.exponentialhistogram.aggregations.support;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.index.fielddata.DocValueBits;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.AggregationErrors;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xpack.exponentialhistogram.ExponentialHistogramFieldMapper;
import org.elasticsearch.xpack.exponentialhistogram.fielddata.ExponentialHistogramValuesReader;
import org.elasticsearch.xpack.exponentialhistogram.fielddata.IndexExponentialHistogramFieldData;

import java.io.IOException;
import java.util.function.Function;

public class ExponentialHistogramValuesSource {

    public abstract static class ExponentialHistogram extends org.elasticsearch.search.aggregations.support.ValuesSource {

        public abstract ExponentialHistogramValuesReader getHistogramValues(LeafReaderContext context) throws IOException;

        public static class Fielddata extends ExponentialHistogram {

            protected final IndexExponentialHistogramFieldData indexFieldData;

            public Fielddata(IndexExponentialHistogramFieldData indexFieldData) {
                this.indexFieldData = indexFieldData;
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
                return indexFieldData.load(context).getBytesValues();
            }

            @Override
            public DocValueBits docsWithValue(LeafReaderContext context) throws IOException {
                ExponentialHistogramValuesReader values = getHistogramValues(context);
                return new DocValueBits() {
                    @Override
                    public boolean advanceExact(int doc) throws IOException {
                        return values.advanceExact(doc);
                    }
                };
            }

            @Override
            protected Function<Rounding, Rounding.Prepared> roundingPreparer(AggregationContext context) {
                throw AggregationErrors.unsupportedRounding(ExponentialHistogramFieldMapper.CONTENT_TYPE);
            }

            public ExponentialHistogramValuesReader getHistogramValues(LeafReaderContext context) throws IOException {
                return indexFieldData.load(context).getHistogramValues();
            }
        }
    }
}
