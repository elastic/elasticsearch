/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.filter;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AdaptingAggregator;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.index.query.AbstractQueryBuilder.parseTopLevelQuery;

public class FilterAggregationBuilder extends AbstractAggregationBuilder<FilterAggregationBuilder> {
    public static final String NAME = "filter";

    private final QueryBuilder filter;

    /**
     * @param name
     *            the name of this aggregation
     * @param filter
     *            Set the filter to use, only documents that match this
     *            filter will fall into the bucket defined by this
     *            {@link Filter} aggregation.
     */
    public FilterAggregationBuilder(String name, QueryBuilder filter) {
        super(name);
        if (filter == null) {
            throw new IllegalArgumentException("[filter] must not be null: [" + name + "]");
        }
        this.filter = filter;
    }

    protected FilterAggregationBuilder(
        FilterAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.filter = clone.filter;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new FilterAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * Read from a stream.
     */
    public FilterAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        filter = in.readNamedWriteable(QueryBuilder.class);
    }

    @Override
    public boolean supportsSampling() {
        return true;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(filter);
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.ONE;
    }

    @Override
    protected AggregationBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder result = Rewriteable.rewrite(filter, queryRewriteContext);
        if (result != filter) {
            return new FilterAggregationBuilder(getName(), result);
        }
        return this;
    }

    @Override
    protected AggregatorFactory doBuild(
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        return new FilterAggregatorFactory(filter, name, context, parent, subFactoriesBuilder, metadata);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (filter != null) {
            filter.toXContent(builder, params);
        }
        return builder;
    }

    public static FilterAggregationBuilder parse(XContentParser parser, String aggregationName) throws IOException {
        QueryBuilder filter = parseTopLevelQuery(parser);
        return new FilterAggregationBuilder(aggregationName, filter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), filter);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        FilterAggregationBuilder other = (FilterAggregationBuilder) obj;
        return Objects.equals(filter, other.filter);
    }

    @Override
    public String getType() {
        return NAME;
    }

    public QueryBuilder getFilter() {
        return filter;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.ZERO;
    }

    public static class FilterAggregatorFactory extends AggregatorFactory {

        private final QueryToFilterAdapter filter;

        public FilterAggregatorFactory(
            QueryBuilder filter,
            String name,
            AggregationContext context,
            AggregatorFactory parent,
            AggregatorFactories.Builder subFactoriesBuilder,
            Map<String, Object> metadata
        ) throws IOException {
            super(name, context, parent, subFactoriesBuilder, metadata);
            this.filter = QueryToFilterAdapter.build(context.searcher(), "1", context.buildQuery(filter));
            ;
        }

        @Override
        protected Aggregator createInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
            throws IOException {
            final var innerAggregator = FiltersAggregator.build(
                name,
                factories,
                List.of(filter),
                false,
                null,
                context,
                parent,
                cardinality,
                metadata
            );
            return new FilterAggregator(name, parent, factories, innerAggregator);
        }
    }

    static class FilterAggregator extends AdaptingAggregator implements SingleBucketAggregator {

        private final String name;
        private final FiltersAggregator innerAggregator;

        FilterAggregator(String name, Aggregator parent, AggregatorFactories subAggregators, FiltersAggregator innerAggregator)
            throws IOException {
            super(parent, subAggregators, aggregatorFactories -> innerAggregator);
            this.name = name;
            this.innerAggregator = innerAggregator;
        }

        @Override
        protected InternalAggregation adapt(InternalAggregation delegateResult) throws IOException {
            InternalFilters innerResult = (InternalFilters) delegateResult;
            var innerBucket = innerResult.getBuckets().get(0);
            return new InternalFilter(name, innerBucket.getDocCount(), innerBucket.getAggregations(), innerResult.getMetadata());
        }

        @Override
        public Aggregator resolveSortPath(AggregationPath.PathElement next, Iterator<AggregationPath.PathElement> path) {
            return resolveSortPathOnValidAgg(next, path);
        }

        @Override
        public BucketComparator bucketComparator(String key, SortOrder order) {
            if (key == null || "doc_count".equals(key)) {
                return (lhs, rhs) -> order.reverseMul() * Long.compare(
                    innerAggregator.bucketDocCount(lhs),
                    innerAggregator.bucketDocCount(rhs)
                );
            } else {
                return super.bucketComparator(key, order);
            }
        }

    }
}
