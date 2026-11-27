/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * An aggregation whose shard result is {@link #RESULT_BYTES} large but that charges nothing on the shard, so only the nodes that
 * merge its results hold that memory.
 */
public class LargeResultAggregationPlugin extends Plugin implements SearchPlugin {

    /** The size of the term of the one bucket of each shard result. */
    public static final long RESULT_BYTES = ByteSizeValue.ofKb(256).getBytes();

    @Override
    public List<AggregationSpec> getAggregations() {
        return List.of(
            new AggregationSpec(
                LargeResultAggregationBuilder.NAME,
                LargeResultAggregationBuilder::new,
                LargeResultAggregationBuilder.PARSER
            ).addResultReader(StringTerms::new)
        );
    }

    public static class LargeResultAggregationBuilder extends AbstractAggregationBuilder<LargeResultAggregationBuilder> {
        static final String NAME = "large_result";

        private static final ObjectParser<LargeResultAggregationBuilder, String> PARSER = ObjectParser.fromBuilder(
            NAME,
            LargeResultAggregationBuilder::new
        );

        public LargeResultAggregationBuilder(String name) {
            super(name);
        }

        LargeResultAggregationBuilder(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        protected void doWriteTo(StreamOutput out) {}

        @Override
        protected AggregatorFactory doBuild(
            AggregationContext context,
            AggregatorFactory parent,
            AggregatorFactories.Builder subFactoriesBuilder
        ) throws IOException {
            return new AggregatorFactory(name, context, parent, subFactoriesBuilder, metadata) {
                @Override
                protected Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata) {
                    return new LargeResultAggregator(name, parent);
                }
            };
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params params) {
            return builder;
        }

        @Override
        protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
            return new LargeResultAggregationBuilder(name);
        }

        @Override
        public BucketCardinality bucketCardinality() {
            return BucketCardinality.NONE;
        }

        @Override
        public String getType() {
            return NAME;
        }

        @Override
        public long bytesToPreallocate() {
            return 0;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.zero();
        }
    }

    /**
     * Extends {@link Aggregator} rather than {@code AggregatorBase}, which charges the breaker on the shard.
     */
    private static class LargeResultAggregator extends Aggregator {
        private final String name;
        private final Aggregator parent;

        private LargeResultAggregator(String name, Aggregator parent) {
            this.name = name;
            this.parent = parent;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Aggregator parent() {
            return parent;
        }

        @Override
        public Aggregator subAggregator(String aggregatorName) {
            return null;
        }

        @Override
        public InternalAggregation[] buildAggregations(LongArray owningBucketOrds) {
            return new InternalAggregation[] { buildEmptyAggregation() };
        }

        @Override
        public void releaseAggregations() {}

        @Override
        public InternalAggregation buildEmptyAggregation() {
            BytesRef term = new BytesRef(new byte[Math.toIntExact(RESULT_BYTES)]);
            StringTerms.Bucket bucket = new StringTerms.Bucket(term, 1, InternalAggregations.EMPTY, false, 0, DocValueFormat.RAW);
            return new StringTerms(
                name,
                BucketOrder.key(true),
                BucketOrder.key(true),
                10,
                1,
                null,
                DocValueFormat.RAW,
                10,
                false,
                0,
                List.of(bucket),
                0L
            );
        }

        @Override
        public void close() {}

        @Override
        public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }

        @Override
        public void preCollection() {}

        @Override
        public void postCollection() {}

        @Override
        public Aggregator[] subAggregators() {
            return new Aggregator[0];
        }
    }
}
