/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SearchTestPlugin extends Plugin implements SearchPlugin {
    private Map<ShardId, ShardIdLatch> shardsLatch;

    public SearchTestPlugin() {
        this.shardsLatch = null;
    }

    public void resetQueryLatch(Map<ShardId, ShardIdLatch> newLatch) {
        shardsLatch = newLatch;
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return Collections.singletonList(
            new QuerySpec<>(BlockingQueryBuilder.NAME,
                in -> new BlockingQueryBuilder(in, shardsLatch),
                p -> BlockingQueryBuilder.fromXContent(p, shardsLatch))
        );
    }

    @Override
    public List<AggregationSpec> getAggregations() {
        return Collections.singletonList(new AggregationSpec(CancellingAggregationBuilder.NAME, CancellingAggregationBuilder::new,
            CancellingAggregationBuilder.PARSER).addResultReader(InternalFilter::new));
    }
}
