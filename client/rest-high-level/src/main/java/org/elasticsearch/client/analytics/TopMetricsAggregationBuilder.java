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

package org.elasticsearch.client.analytics;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Builds the Top Metrics aggregation request.
 * 
 * NOTE: This extends {@linkplain AbstractAggregationBuilder} for compatibility
 * with {@link SearchSourceBuilder#aggregation(AggregationBuilder)} but it
 * doesn't support any "server" side things like
 * {@linkplain Writeable#writeTo(StreamOutput)},
 * {@linkplain AggregationBuilder#rewrite(QueryRewriteContext)}, or
 * {@linkplain AbstractAggregationBuilder#build(QueryShardContext, AggregatorFactory)}.
 */
public class TopMetricsAggregationBuilder extends AbstractAggregationBuilder<TopMetricsAggregationBuilder> {
    public static final String NAME = "top_metrics";

    private final SortBuilder<?> sort;
    private final String metric;

    /**
     * Build the request.
     * @param name the name of the metric
     * @param sort the sort key used to select the top metrics
     * @param metric the name of the field to select
     */
    public TopMetricsAggregationBuilder(String name, SortBuilder<?> sort, String metric) {
        super(name);
        this.sort = sort;
        this.metric = metric;
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
            builder.startObject("metric").field("field", metric).endObject();
        }
        return builder.endObject();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected AggregatorFactory doBuild(QueryShardContext queryShardContext, AggregatorFactory parent, Builder subfactoriesBuilder)
            throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metaData) {
        throw new UnsupportedOperationException();
    }
}
