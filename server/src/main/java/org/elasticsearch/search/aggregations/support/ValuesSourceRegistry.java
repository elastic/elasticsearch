/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.NumericHistogramAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.RangeHistogramAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/*
This is a _very_ crude prototype for the ValuesSourceRegistry which basically hard-codes everything.  The intent is to define the API
for aggregations using the registry to resolve aggregators.
 */
public enum ValuesSourceRegistry {
    INSTANCE {
        @Override
        public AggregatorSupplier getAggregator(ValuesSource valuesSource, String aggregationName) {
            if (aggregationName.equals(HistogramAggregationBuilder.NAME)) {
                if (valuesSource instanceof ValuesSource.Numeric) {
                    return new HistogramAggregatorSupplier() {
                        @Override
                        public Aggregator build(String name, AggregatorFactories factories, double interval, double offset,
                                                BucketOrder order, boolean keyed, long minDocCount, double minBound, double maxBound,
                                                ValuesSource valuesSource, DocValueFormat formatter, SearchContext context,
                                                Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                                                Map<String, Object> metaData) throws IOException {
                            return new NumericHistogramAggregator(name, factories, interval, offset, order, keyed, minDocCount, minBound,
                                maxBound, (ValuesSource.Numeric) valuesSource, formatter, context, parent, pipelineAggregators, metaData);
                        }
                    };
                } else if (valuesSource instanceof ValuesSource.Range) {
                    return new HistogramAggregatorSupplier() {
                        @Override
                        public Aggregator build(String name, AggregatorFactories factories, double interval, double offset,
                                                BucketOrder order, boolean keyed, long minDocCount, double minBound, double maxBound,
                                                ValuesSource valuesSource, DocValueFormat formatter, SearchContext context,
                                                Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                                                Map<String, Object> metaData) throws IOException {
                            ValuesSource.Range rangeValueSource = (ValuesSource.Range) valuesSource;
                            if (rangeValueSource.rangeType().isNumeric() == false) {
                                throw new IllegalArgumentException("Expected numeric range type but found non-numeric range ["
                                    + rangeValueSource.rangeType().name + "]");
                            }
                            return new RangeHistogramAggregator(name, factories, interval, offset, order, keyed, minDocCount, minBound,
                                maxBound, rangeValueSource, formatter, context, parent, pipelineAggregators, metaData);
                        }
                    };
                }
            }
            // TODO: Error message should list valid ValuesSource types
            throw new AggregationExecutionException("ValuesSource type " + valuesSource.toString() + " is not supported for aggregation" +
                aggregationName);
        }
    };

    public abstract AggregatorSupplier getAggregator(ValuesSource valuesSource, String aggregationName);
}
