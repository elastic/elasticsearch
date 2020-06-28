/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.search;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorBase;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FailReduceAggPlugin extends Plugin implements SearchPlugin {
    @Override
    public List<AggregationSpec> getAggregations() {
        AggregationSpec aggregationSpec = new AggregationSpec(
            FAIL_REDUCE_AGG,
            FailReduceAggregationBuilder::new,
            (ContextParser<String, AggregationBuilder>) (parser, name) -> new FailReduceAggregationBuilder(name));
        aggregationSpec.addResultReader(FAIL_REDUCE_AGG, FailReduceInternalAgg::new);
        return Collections.singletonList(aggregationSpec);
    }

    private static final String FAIL_REDUCE_AGG = "fail_reduce";

    public static class FailReduceAggregationBuilder extends AbstractAggregationBuilder<FailReduceAggregationBuilder> {

        public FailReduceAggregationBuilder(String name) {
            super(name);
        }

        public FailReduceAggregationBuilder(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        protected void doWriteTo(StreamOutput out) {

        }

        @Override
        protected AggregatorFactory doBuild(QueryShardContext queryShardContext, AggregatorFactory parent,
                                            AggregatorFactories.Builder subfactoriesBuilder) throws IOException {
            return new AggregatorFactory(name, queryShardContext, parent, subfactoriesBuilder, metadata) {
                @Override
                protected Aggregator createInternal(SearchContext searchContext,
                                                    Aggregator parent,
                                                    boolean collectsFromSingleBucket,
                                                    Map<String, Object> metadata) throws IOException {
                    return new FailReduceAggregator(name, factories, searchContext, parent, metadata);
                }
            };
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params params) {
            return builder;
        }

        @Override
        protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
            return new FailReduceAggregationBuilder(name);
        }

        @Override
        public BucketCardinality bucketCardinality() {
            return BucketCardinality.NONE;
        }

        @Override
        public String getType() {
            return FAIL_REDUCE_AGG;
        }
    }

    public static class FailReduceAggregator extends AggregatorBase {

        /**
         * Constructs a new Aggregator.
         *
         * @param name      The name of the aggregation
         * @param factories The factories for all the sub-aggregators under this aggregator
         * @param context   The aggregation context
         * @param parent    The parent aggregator (may be {@code null} for top level aggregators)
         * @param metadata  The metadata associated with this aggregator
         */
        protected FailReduceAggregator(String name, AggregatorFactories factories, SearchContext context, Aggregator parent,
                                       Map<String, Object> metadata) throws IOException {
            super(name, factories, context, parent, metadata);
        }

        @Override
        protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        @Override
        public InternalAggregation[] buildAggregations(long[] owningBucketOrds) {
            return new InternalAggregation[]{new FailReduceInternalAgg()};
        }

        @Override
        public InternalAggregation buildEmptyAggregation() {
            return new FailReduceInternalAgg();
        }
    }

    public static class FailReduceInternalAgg extends InternalAggregation {

        FailReduceInternalAgg() {
            super(FAIL_REDUCE_AGG, Collections.emptyMap());
        }

        FailReduceInternalAgg(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        protected void doWriteTo(StreamOutput out) {

        }

        @Override
        public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
            if (reduceContext.isFinalReduce()) {
                throw new CircuitBreakingException("boom", CircuitBreaker.Durability.TRANSIENT);
            }
            return this;
        }

        @Override
        public Object getProperty(List<String> path) {
            return null;
        }

        @Override
        public XContentBuilder doXContentBody(XContentBuilder builder, Params params) {
            return builder;
        }

        @Override
        public String getWriteableName() {
            return FAIL_REDUCE_AGG;
        }
    }
}
