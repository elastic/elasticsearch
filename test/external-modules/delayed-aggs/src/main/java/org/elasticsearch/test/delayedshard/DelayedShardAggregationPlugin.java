/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.delayedshard;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;

import java.util.List;

import static java.util.Collections.singletonList;

/**
 * Test plugin that allows to delay aggregations on shards with a configurable time
 */
public class DelayedShardAggregationPlugin extends Plugin implements SearchPlugin {
    public DelayedShardAggregationPlugin() {}

    @Override
    public List<AggregationSpec> getAggregations() {
        return singletonList(
            new AggregationSpec(
                DelayedShardAggregationBuilder.NAME,
                DelayedShardAggregationBuilder::new,
                DelayedShardAggregationBuilder.PARSER
            ).addResultReader(InternalFilter::new)
        );
    }
}
