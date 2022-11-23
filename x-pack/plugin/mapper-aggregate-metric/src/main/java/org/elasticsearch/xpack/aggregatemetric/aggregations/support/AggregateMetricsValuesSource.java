/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.aggregatemetric.aggregations.support;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.index.fielddata.DocValueBits;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xpack.aggregatemetric.fielddata.IndexAggregateDoubleMetricFieldData;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper;
import org.elasticsearch.xpack.aggregatemetric.mapper.AggregateDoubleMetricFieldMapper.Metric;

import java.io.IOException;
import java.util.function.Function;

public class AggregateMetricsValuesSource {
    public abstract static class AggregateDoubleMetric extends org.elasticsearch.search.aggregations.support.ValuesSource {

        public abstract SortedNumericDoubleValues getAggregateMetricValues(LeafReaderContext context, Metric metric) throws IOException;

        public static class Fielddata extends AggregateDoubleMetric {

            protected final IndexAggregateDoubleMetricFieldData indexFieldData;

            public Fielddata(IndexAggregateDoubleMetricFieldData indexFieldData) {
                this.indexFieldData = indexFieldData;
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
                return indexFieldData.load(context).getBytesValues();
            }

            @Override
            public DocValueBits docsWithValue(LeafReaderContext context) throws IOException {
                SortedNumericDoubleValues values = getAggregateMetricValues(context, null);
                return new DocValueBits() {
                    @Override
                    public boolean advanceExact(int doc) throws IOException {
                        return values.advanceExact(doc);
                    }
                };
            }

            @Override
            protected Function<Rounding, Rounding.Prepared> roundingPreparer(AggregationContext context) throws IOException {
                throw new AggregationExecutionException("Can't round an [" + AggregateDoubleMetricFieldMapper.CONTENT_TYPE + "]");
            }

            public SortedNumericDoubleValues getAggregateMetricValues(LeafReaderContext context, Metric metric) throws IOException {
                return indexFieldData.load(context).getAggregateMetricValues(metric);
            }
        }
    }
}
