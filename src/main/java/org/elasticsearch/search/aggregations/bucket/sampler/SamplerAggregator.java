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
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.BestDocsDeferringCollector;
import org.elasticsearch.search.aggregations.bucket.DeferringBucketCollector;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

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


    public enum ExecutionMode {

        MAP(new ParseField("map")) {

            @Override
            Aggregator create(String name, AggregatorFactories factories, int shardSize, int maxDocsPerValue, ValuesSource valuesSource,
                    AggregationContext context, Aggregator parent, Map<String, Object> metaData) throws IOException {

                return new DiversifiedMapSamplerAggregator(name, shardSize, factories, context, parent, metaData, valuesSource,
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
                    AggregationContext context, Aggregator parent, Map<String, Object> metaData) throws IOException {

                return new DiversifiedBytesHashSamplerAggregator(name, shardSize, factories, context, parent, metaData, valuesSource,
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
                    AggregationContext context, Aggregator parent, Map<String, Object> metaData) throws IOException {
                return new DiversifiedOrdinalsSamplerAggregator(name, shardSize, factories, context, parent, metaData,
                        (ValuesSource.Bytes.WithOrdinals.FieldData) valuesSource, maxDocsPerValue);
            }

            @Override
            boolean needsGlobalOrdinals() {
                return true;
            }

        };

        public static ExecutionMode fromString(String value) {
            for (ExecutionMode mode : values()) {
                if (mode.parseField.match(value)) {
                    return mode;
                }
            }
            throw new ElasticsearchIllegalArgumentException("Unknown `execution_hint`: [" + value + "], expected any of " + values());
        }

        private final ParseField parseField;

        ExecutionMode(ParseField parseField) {
            this.parseField = parseField;
        }

        abstract Aggregator create(String name, AggregatorFactories factories, int shardSize, int maxDocsPerValue, ValuesSource valuesSource,
                AggregationContext context, Aggregator parent, Map<String, Object> metaData) throws IOException;

        abstract boolean needsGlobalOrdinals();

        @Override
        public String toString() {
            return parseField.getPreferredName();
        }
    }    
    

    protected final int shardSize;
    protected BestDocsDeferringCollector bdd;

    public SamplerAggregator(String name, int shardSize, AggregatorFactories factories,
            AggregationContext aggregationContext, Aggregator parent, Map<String, Object> metaData) throws IOException {
        super(name, factories, aggregationContext, parent, metaData);
        this.shardSize = shardSize;
    }

    @Override
    public boolean needsScores() {
        return true;
    }

    @Override
    public DeferringBucketCollector getDeferringCollector() {
        bdd = new BestDocsDeferringCollector(shardSize);
        return bdd;

    }


    @Override
    protected boolean shouldDefer(Aggregator aggregator) {
        return true;
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        runDeferredCollections(owningBucketOrdinal);
        return new InternalSampler(name, bdd == null ? 0 : bdd.getDocCount(), bucketAggregations(owningBucketOrdinal), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalSampler(name, 0, buildEmptySubAggregations(), metaData());
    }

    public static class Factory extends AggregatorFactory {

        private int shardSize;

        public Factory(String name, int shardSize) {
            super(name, InternalSampler.TYPE.name());
            this.shardSize = shardSize;
        }

        @Override
        public Aggregator createInternal(AggregationContext context, Aggregator parent, boolean collectsFromSingleBucket,
                Map<String, Object> metaData) throws IOException {

            if (collectsFromSingleBucket == false) {
                return asMultiBucketAggregator(this, context, parent);
            }
            return new SamplerAggregator(name, shardSize, factories, context, parent, metaData);
        }

    }

    public static class DiversifiedFactory extends ValuesSourceAggregatorFactory<ValuesSource> {

        private int shardSize;
        private int maxDocsPerValue;
        private String executionHint;

        public DiversifiedFactory(String name, int shardSize, String executionHint, ValuesSourceConfig vsConfig, int maxDocsPerValue) {
            super(name, InternalSampler.TYPE.name(), vsConfig);
            this.shardSize = shardSize;
            this.maxDocsPerValue = maxDocsPerValue;
            this.executionHint = executionHint;
        }

        @Override
        protected Aggregator doCreateInternal(ValuesSource valuesSource, AggregationContext context, Aggregator parent,
                boolean collectsFromSingleBucket, Map<String, Object> metaData) throws IOException {

            if (collectsFromSingleBucket == false) {
                return asMultiBucketAggregator(this, context, parent);
            }


            if (valuesSource instanceof ValuesSource.Numeric) {
                return new DiversifiedNumericSamplerAggregator(name, shardSize, factories, context, parent, metaData,
                        (Numeric) valuesSource, maxDocsPerValue);
            }
            
            if (valuesSource instanceof ValuesSource.Bytes) {
                ExecutionMode execution = null;
                if (executionHint != null) {
                    execution = ExecutionMode.fromString(executionHint);
                }

                // In some cases using ordinals is just not supported: override
                // it
                if(execution==null){
                    execution = ExecutionMode.GLOBAL_ORDINALS;
                }
                if ((execution.needsGlobalOrdinals()) && (!(valuesSource instanceof ValuesSource.Bytes.WithOrdinals))) {
                    execution = ExecutionMode.MAP;
                }
                return execution.create(name, factories, shardSize, maxDocsPerValue, valuesSource, context, parent, metaData);
            }
            
            throw new AggregationExecutionException("Sampler aggregation cannot be applied to field [" + config.fieldContext().field() +
                    "]. It can only be applied to numeric or string fields.");
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent, Map<String, Object> metaData)
                throws IOException {
            final UnmappedSampler aggregation = new UnmappedSampler(name, metaData);

            return new NonCollectingAggregator(name, aggregationContext, parent, factories, metaData) {
                @Override
                public InternalAggregation buildEmptyAggregation() {
                    return aggregation;
                }
            };
        }

    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (bdd == null) {
            throw new AggregationExecutionException("Sampler aggregation must be used with child aggregations.");
        }
        return bdd.getLeafCollector(ctx);
    }

}

