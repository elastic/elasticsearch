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
package org.elasticsearch.search.aggregations.bucket.sampler;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.AggregatorBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.BestDocsDeferringCollector;
import org.elasticsearch.search.aggregations.bucket.DeferringBucketCollector;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Aggregate on only the top-scoring docs on a shard.
 *
 * TODO currently the diversity feature of this agg offers only 'script' and
 * 'field' as a means of generating a de-dup value. In future it would be nice
 * if users could use any of the "bucket" aggs syntax (geo, date histogram...)
 * as the basis for generating de-dup values. Their syntax for creating bucket
 * values would be preferable to users having to recreate this logic in a
 * 'script' e.g. to turn a datetime in milliseconds into a month key value.
 */
public class SamplerAggregator extends SingleBucketAggregator {

    public static final ParseField SHARD_SIZE_FIELD = new ParseField("shard_size");
    public static final ParseField MAX_DOCS_PER_VALUE_FIELD = new ParseField("max_docs_per_value");
    public static final ParseField EXECUTION_HINT_FIELD = new ParseField("execution_hint");


    public enum ExecutionMode {

        MAP(new ParseField("map")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, int shardSize, int maxDocsPerValue, ValuesSource valuesSource,
                    AggregationContext context, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                    Map<String, Object> metaData) throws IOException {

                return new DiversifiedMapSamplerAggregator(name, shardSize, factories, context, parent, pipelineAggregators, metaData,
                        valuesSource,
                        maxDocsPerValue);
            }

            @Override
            boolean needsGlobalOrdinals() {
                return false;
            }

        },
        BYTES_HASH(new ParseField("bytes_hash")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, int shardSize, int maxDocsPerValue, ValuesSource valuesSource,
                    AggregationContext context, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                    Map<String, Object> metaData) throws IOException {

                return new DiversifiedBytesHashSamplerAggregator(name, shardSize, factories, context, parent, pipelineAggregators,
                        metaData,
                        valuesSource,
                        maxDocsPerValue);
            }

            @Override
            boolean needsGlobalOrdinals() {
                return false;
            }

        },
        GLOBAL_ORDINALS(new ParseField("global_ordinals")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, int shardSize, int maxDocsPerValue, ValuesSource valuesSource,
                    AggregationContext context, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                    Map<String, Object> metaData) throws IOException {
                return new DiversifiedOrdinalsSamplerAggregator(name, shardSize, factories, context, parent, pipelineAggregators, metaData,
                        (ValuesSource.Bytes.WithOrdinals.FieldData) valuesSource, maxDocsPerValue);
            }

            @Override
            boolean needsGlobalOrdinals() {
                return true;
            }

        };

        public static ExecutionMode fromString(String value, ParseFieldMatcher parseFieldMatcher) {
            for (ExecutionMode mode : values()) {
                if (parseFieldMatcher.match(value, mode.parseField)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("Unknown `execution_hint`: [" + value + "], expected any of " + values());
        }

        private final ParseField parseField;

        ExecutionMode(ParseField parseField) {
            this.parseField = parseField;
        }

        abstract Aggregator create(String name, AggregatorFactories factories, int shardSize, int maxDocsPerValue, ValuesSource valuesSource,
 AggregationContext context, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                Map<String, Object> metaData) throws IOException;

        abstract boolean needsGlobalOrdinals();

        @Override
        public String toString() {
            return parseField.getPreferredName();
        }
    }


    protected final int shardSize;
    protected BestDocsDeferringCollector bdd;

    public SamplerAggregator(String name, int shardSize, AggregatorFactories factories, AggregationContext aggregationContext,
            Aggregator parent, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) throws IOException {
        super(name, factories, aggregationContext, parent, pipelineAggregators, metaData);
        this.shardSize = shardSize;
    }

    @Override
    public boolean needsScores() {
        return true;
    }

    @Override
    public DeferringBucketCollector getDeferringCollector() {
        bdd = new BestDocsDeferringCollector(shardSize, context.bigArrays());
        return bdd;

    }


    @Override
    protected boolean shouldDefer(Aggregator aggregator) {
        return true;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        runDeferredCollections(owningBucketOrdinal);
        return new InternalSampler(name, bdd == null ? 0 : bdd.getDocCount(owningBucketOrdinal), bucketAggregations(owningBucketOrdinal),
                pipelineAggregators(),
                metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalSampler(name, 0, buildEmptySubAggregations(), pipelineAggregators(), metaData());
    }

    public static class SamplerAggregatorBuilder extends AggregatorBuilder<SamplerAggregatorBuilder> {

        public static final int DEFAULT_SHARD_SAMPLE_SIZE = 100;

        private int shardSize = DEFAULT_SHARD_SAMPLE_SIZE;

        public SamplerAggregatorBuilder(String name) {
            super(name, InternalSampler.TYPE);
        }

        /**
         * Set the max num docs to be returned from each shard.
         */
        public SamplerAggregatorBuilder shardSize(int shardSize) {
            this.shardSize = shardSize;
            return this;
        }

        /**
         * Get the max num docs to be returned from each shard.
         */
        public int shardSize() {
            return shardSize;
        }

        @Override
        protected AggregatorFactory<?> doBuild(AggregationContext context) {
            return new SamplerAggregatorFactory(name, type, shardSize);
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(SHARD_SIZE_FIELD.getPreferredName(), shardSize);
            builder.endObject();
            return builder;
        }

        @Override
        protected SamplerAggregatorBuilder doReadFrom(String name, StreamInput in) throws IOException {
            SamplerAggregatorBuilder factory = new SamplerAggregatorBuilder(name);
            factory.shardSize = in.readVInt();
            return factory;
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            out.writeVInt(shardSize);
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(shardSize);
        }

        @Override
        protected boolean doEquals(Object obj) {
            SamplerAggregatorBuilder other = (SamplerAggregatorBuilder) obj;
            return Objects.equals(shardSize, other.shardSize);
        }

    }

    public static class DiversifiedAggregatorBuilder extends ValuesSourceAggregatorBuilder<ValuesSource, DiversifiedAggregatorBuilder> {

        public static final Type TYPE = new Type("diversified_sampler");

        public static final int MAX_DOCS_PER_VALUE_DEFAULT = 1;

        private int shardSize = SamplerAggregatorBuilder.DEFAULT_SHARD_SAMPLE_SIZE;
        private int maxDocsPerValue = MAX_DOCS_PER_VALUE_DEFAULT;
        private String executionHint = null;

        public DiversifiedAggregatorBuilder(String name) {
            super(name, TYPE, ValuesSourceType.ANY, null);
        }

        /**
         * Set the max num docs to be returned from each shard.
         */
        public DiversifiedAggregatorBuilder shardSize(int shardSize) {
            this.shardSize = shardSize;
            return this;
        }

        /**
         * Get the max num docs to be returned from each shard.
         */
        public int shardSize() {
            return shardSize;
        }

        /**
         * Set the max num docs to be returned per value.
         */
        public DiversifiedAggregatorBuilder maxDocsPerValue(int maxDocsPerValue) {
            this.maxDocsPerValue = maxDocsPerValue;
            return this;
        }

        /**
         * Get the max num docs to be returned per value.
         */
        public int maxDocsPerValue() {
            return maxDocsPerValue;
        }

        /**
         * Set the execution hint.
         */
        public DiversifiedAggregatorBuilder executionHint(String executionHint) {
            this.executionHint = executionHint;
            return this;
        }

        /**
         * Get the execution hint.
         */
        public String executionHint() {
            return executionHint;
        }

        @Override
        protected ValuesSourceAggregatorFactory<ValuesSource, ?> innerBuild(AggregationContext context,
                ValuesSourceConfig<ValuesSource> config) {
            return new DiversifiedAggregatorFactory(name, TYPE, config, shardSize, maxDocsPerValue, executionHint);
        }

        @Override
        protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
            builder.field(SHARD_SIZE_FIELD.getPreferredName(), shardSize);
            builder.field(MAX_DOCS_PER_VALUE_FIELD.getPreferredName(), maxDocsPerValue);
            if (executionHint != null) {
                builder.field(EXECUTION_HINT_FIELD.getPreferredName(), executionHint);
            }
            return builder;
        }

        @Override
        protected DiversifiedAggregatorBuilder innerReadFrom(String name, ValuesSourceType valuesSourceType,
                ValueType targetValueType, StreamInput in) throws IOException {
            DiversifiedAggregatorBuilder factory = new DiversifiedAggregatorBuilder(name);
            factory.shardSize = in.readVInt();
            factory.maxDocsPerValue = in.readVInt();
            factory.executionHint = in.readOptionalString();
            return factory;
        }

        @Override
        protected void innerWriteTo(StreamOutput out) throws IOException {
            out.writeVInt(shardSize);
            out.writeVInt(maxDocsPerValue);
            out.writeOptionalString(executionHint);
        }

        @Override
        protected int innerHashCode() {
            return Objects.hash(shardSize, maxDocsPerValue, executionHint);
        }

        @Override
        protected boolean innerEquals(Object obj) {
            DiversifiedAggregatorBuilder other = (DiversifiedAggregatorBuilder) obj;
            return Objects.equals(shardSize, other.shardSize)
                    && Objects.equals(maxDocsPerValue, other.maxDocsPerValue)
                    && Objects.equals(executionHint, other.executionHint);
        }
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (bdd == null) {
            throw new AggregationExecutionException("Sampler aggregation must be used with child aggregations.");
        }
        return bdd.getLeafCollector(ctx);
    }

    @Override
    protected void doClose() {
        Releasables.close(bdd);
        super.doClose();
    }

}

