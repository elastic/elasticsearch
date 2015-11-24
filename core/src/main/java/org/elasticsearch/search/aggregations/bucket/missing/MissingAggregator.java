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
package org.elasticsearch.search.aggregations.bucket.missing;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class MissingAggregator extends SingleBucketAggregator {

    private final ValuesSource valuesSource;

    public MissingAggregator(String name, AggregatorFactories factories, ValuesSource valuesSource,
            AggregationContext aggregationContext, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {
        super(name, factories, aggregationContext, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {

        final Bits docsWithValue;
        if (valuesSource != null) {
            docsWithValue = valuesSource.docsWithValue(ctx);
        } else {
            docsWithValue = new Bits.MatchNoBits(ctx.reader().maxDoc());
        }
        return new LeafBucketCollectorBase(sub, docsWithValue) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (docsWithValue != null && !docsWithValue.get(doc)) {
                    collectBucket(sub, doc, bucket);
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        return new InternalMissing(name, bucketDocCount(owningBucketOrdinal), bucketAggregations(owningBucketOrdinal), pipelineAggregators(),
                metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalMissing(name, 0, buildEmptySubAggregations(), pipelineAggregators(), metaData());
    }

    public static class Factory<VS extends ValuesSource> extends ValuesSourceAggregatorFactory<VS> {

        public Factory(String name, ValuesSourceType valuesSourceType, ValueType valueType) {
            super(name, InternalMissing.TYPE, valuesSourceType, valueType);
        }

        @Override
        protected MissingAggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                Map<String, Object> metaData) throws IOException {
            return new MissingAggregator(name, factories, null, aggregationContext, parent, pipelineAggregators, metaData);
        }

        @Override
        protected MissingAggregator doCreateInternal(VS valuesSource, AggregationContext aggregationContext, Aggregator parent,
                boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
            return new MissingAggregator(name, factories, valuesSource, aggregationContext, parent, pipelineAggregators, metaData);
        }

        @Override
        protected ValuesSourceAggregatorFactory<VS> innerReadFrom(String name, ValuesSourceType valuesSourceType,
                ValueType targetValueType, StreamInput in) {
            return new ValueCountAggregator.Factory<VS>(name, valuesSourceType, targetValueType);
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


