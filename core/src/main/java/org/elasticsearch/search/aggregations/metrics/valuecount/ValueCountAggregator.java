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
package org.elasticsearch.search.aggregations.metrics.valuecount;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A field data based aggregator that counts the number of values a specific field has within the aggregation context.
 *
 * This aggregator works in a multi-bucket mode, that is, when serves as a sub-aggregator, a single aggregator instance aggregates the
 * counts for all buckets owned by the parent aggregator)
 */
public class ValueCountAggregator extends NumericMetricsAggregator.SingleValue {

    final ValuesSource valuesSource;
    final ValueFormatter formatter;

    // a count per bucket
    LongArray counts;

    public ValueCountAggregator(String name, ValuesSource valuesSource, ValueFormatter formatter,
            AggregationContext aggregationContext, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData)
            throws IOException {
        super(name, aggregationContext, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.formatter = formatter;
        if (valuesSource != null) {
            counts = context.bigArrays().newLongArray(1, true);
        }
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {

            @Override
            public void collect(int doc, long bucket) throws IOException {
                counts = bigArrays.grow(counts, bucket + 1);
                values.setDocument(doc);
                counts.increment(bucket, values.count());
            }

        };
    }

    @Override
    public double metric(long owningBucketOrd) {
        return valuesSource == null ? 0 : counts.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= counts.size()) {
            return buildEmptyAggregation();
        }
        return new InternalValueCount(name, counts.get(bucket), formatter, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalValueCount(name, 0L, formatter, pipelineAggregators(), metaData());
    }

    @Override
    public void doClose() {
        Releasables.close(counts);
    }

    public static class ValueCountAggregatorBuilder
            extends ValuesSourceAggregatorBuilder.LeafOnly<ValuesSource, ValueCountAggregatorBuilder> {

        static final ValueCountAggregatorBuilder PROTOTYPE = new ValueCountAggregatorBuilder("", null);

        public ValueCountAggregatorBuilder(String name, ValueType targetValueType) {
            super(name, InternalValueCount.TYPE, ValuesSourceType.ANY, targetValueType);
        }

        @Override
        protected ValueCountAggregatorFactory innerBuild(AggregationContext context, ValuesSourceConfig<ValuesSource> config,
                AggregatorFactory<?> parent, AggregatorFactories.Builder subFactoriesBuilder) throws IOException {
            return new ValueCountAggregatorFactory(name, type, config, context, parent, subFactoriesBuilder, metaData);
        }

        @Override
        protected ValuesSourceAggregatorBuilder<ValuesSource, ValueCountAggregatorBuilder> innerReadFrom(String name,
                ValuesSourceType valuesSourceType, ValueType targetValueType, StreamInput in) {
            return new ValueCountAggregator.ValueCountAggregatorBuilder(name, targetValueType);
        }

        @Override
        protected void innerWriteTo(StreamOutput out) {
            // Do nothing, no extra state to write to stream
        }

        @Override
        public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }

        @Override
        protected int innerHashCode() {
            return 0;
        }

        @Override
        protected boolean innerEquals(Object obj) {
            return true;
        }

    }

}
