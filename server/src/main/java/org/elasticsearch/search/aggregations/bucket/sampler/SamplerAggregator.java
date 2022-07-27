/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.sampler;

import org.apache.lucene.misc.search.DiversifiedTopDocsCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.DeferringBucketCollector;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xcontent.ParseField;

import java.io.IOException;
import java.util.Arrays;
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
public class SamplerAggregator extends DeferableBucketAggregator implements SingleBucketAggregator {

    public static final ParseField SHARD_SIZE_FIELD = new ParseField("shard_size");
    public static final ParseField MAX_DOCS_PER_VALUE_FIELD = new ParseField("max_docs_per_value");
    public static final ParseField EXECUTION_HINT_FIELD = new ParseField("execution_hint");

    static final long SCOREDOCKEY_SIZE = RamUsageEstimator.shallowSizeOfInstance(DiversifiedTopDocsCollector.ScoreDocKey.class);

    public enum ExecutionMode {

        MAP(new ParseField("map")) {

            @Override
            Aggregator create(
                String name,
                AggregatorFactories factories,
                int shardSize,
                int maxDocsPerValue,
                ValuesSourceConfig valuesSourceConfig,
                AggregationContext context,
                Aggregator parent,
                Map<String, Object> metadata
            ) throws IOException {

                return new DiversifiedMapSamplerAggregator(
                    name,
                    shardSize,
                    factories,
                    context,
                    parent,
                    metadata,
                    valuesSourceConfig,
                    maxDocsPerValue
                );
            }

            @Override
            boolean needsGlobalOrdinals() {
                return false;
            }

        },
        BYTES_HASH(new ParseField("bytes_hash")) {

            @Override
            Aggregator create(
                String name,
                AggregatorFactories factories,
                int shardSize,
                int maxDocsPerValue,
                ValuesSourceConfig valuesSourceConfig,
                AggregationContext context,
                Aggregator parent,
                Map<String, Object> metadata
            ) throws IOException {

                return new DiversifiedBytesHashSamplerAggregator(
                    name,
                    shardSize,
                    factories,
                    context,
                    parent,
                    metadata,
                    valuesSourceConfig,
                    maxDocsPerValue
                );
            }

            @Override
            boolean needsGlobalOrdinals() {
                return false;
            }

        },
        GLOBAL_ORDINALS(new ParseField("global_ordinals")) {

            @Override
            Aggregator create(
                String name,
                AggregatorFactories factories,
                int shardSize,
                int maxDocsPerValue,
                ValuesSourceConfig valuesSourceConfig,
                AggregationContext context,
                Aggregator parent,
                Map<String, Object> metadata
            ) throws IOException {
                return new DiversifiedOrdinalsSamplerAggregator(
                    name,
                    shardSize,
                    factories,
                    context,
                    parent,
                    metadata,
                    valuesSourceConfig,
                    maxDocsPerValue
                );
            }

            @Override
            boolean needsGlobalOrdinals() {
                return true;
            }

        };

        public static ExecutionMode fromString(String value) {
            for (ExecutionMode mode : values()) {
                if (mode.parseField.match(value, LoggingDeprecationHandler.INSTANCE)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("Unknown `execution_hint`: [" + value + "], expected any of " + Arrays.toString(values()));
        }

        private final ParseField parseField;

        ExecutionMode(ParseField parseField) {
            this.parseField = parseField;
        }

        abstract Aggregator create(
            String name,
            AggregatorFactories factories,
            int shardSize,
            int maxDocsPerValue,
            ValuesSourceConfig valuesSourceConfig,
            AggregationContext context,
            Aggregator parent,
            Map<String, Object> metadata
        ) throws IOException;

        abstract boolean needsGlobalOrdinals();

        @Override
        public String toString() {
            return parseField.getPreferredName();
        }
    }

    protected final int shardSize;
    protected BestDocsDeferringCollector bdd;

    SamplerAggregator(
        String name,
        int shardSize,
        AggregatorFactories factories,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, metadata);
        // Make sure we do not allow size > maxDoc, to prevent accidental OOM
        this.shardSize = Math.min(shardSize, searcher().getIndexReader().maxDoc());
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE;
    }

    @Override
    public DeferringBucketCollector buildDeferringCollector() {
        bdd = new BestDocsDeferringCollector(shardSize, bigArrays(), this::addRequestCircuitBreakerBytes);
        return bdd;
    }

    @Override
    protected boolean shouldDefer(Aggregator aggregator) {
        return true;
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return buildAggregationsForSingleBucket(
            owningBucketOrds,
            (owningBucketOrd, subAggregationResults) -> new InternalSampler(
                name,
                bdd == null ? 0 : bdd.getDocCount(owningBucketOrd),
                subAggregationResults,
                metadata()
            )
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalSampler(name, 0, buildEmptySubAggregations(), metadata());
    }

    @Override
    protected LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        if (bdd == null) {
            throw new AggregationExecutionException("Sampler aggregation must be used with child aggregations.");
        }
        return bdd.getLeafCollector(aggCtx);
    }

    @Override
    protected void doClose() {
        Releasables.close(bdd);
        super.doClose();
    }

}
