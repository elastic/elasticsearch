/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.analytics;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Builds the Top Metrics aggregation request.
 * <p>
 * NOTE: This extends {@linkplain AbstractAggregationBuilder} for compatibility
 * with {@link SearchSourceBuilder#aggregation(AggregationBuilder)} but it
 * doesn't support any "server" side things like
 * {@linkplain Writeable#writeTo(StreamOutput)},
 * {@linkplain AggregationBuilder#rewrite(QueryRewriteContext)}, or
 * {@linkplain AbstractAggregationBuilder#build(AggregationContext, AggregatorFactory)}.
 */
public class TopMetricsAggregationBuilder extends AbstractAggregationBuilder<TopMetricsAggregationBuilder> {
    public static final String NAME = "top_metrics";

    private final SortBuilder<?> sort;
    private final int size;
    private final List<String> metrics;

    /**
     * Build the request.
     * @param name the name of the metric
     * @param sort the sort key used to select the top metrics
     * @param size number of results to return per bucket
     * @param metrics the names of the fields to select
     */
    public TopMetricsAggregationBuilder(String name, SortBuilder<?> sort, int size, String... metrics) {
        super(name);
        this.sort = sort;
        this.size = size;
        this.metrics = Arrays.asList(metrics);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.startArray("sort");
            sort.toXContent(builder, params);
            builder.endArray();
            builder.field("size", size);
            builder.startArray("metrics");
            for (String metric : metrics) {
                builder.startObject().field("field", metric).endObject();
            }
            builder.endArray();
        }
        return builder.endObject();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.NONE;
    }

    @Override
    protected AggregatorFactory doBuild(AggregationContext context, AggregatorFactory parent, Builder subfactoriesBuilder)
        throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_7_7_0;
    }
}
