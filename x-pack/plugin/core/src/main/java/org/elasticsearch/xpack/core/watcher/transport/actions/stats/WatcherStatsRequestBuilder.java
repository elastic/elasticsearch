/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.stats;

import org.elasticsearch.action.support.nodes.NodesOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Watcher stats request builder.
 */
public class WatcherStatsRequestBuilder extends NodesOperationRequestBuilder<WatcherStatsRequest, WatcherStatsResponse,
        WatcherStatsRequestBuilder> {

    public WatcherStatsRequestBuilder(ElasticsearchClient client) {
        super(client, WatcherStatsAction.INSTANCE, new WatcherStatsRequest());
    }

    public WatcherStatsRequestBuilder setIncludeCurrentWatches(boolean includeCurrentWatches) {
        request().includeCurrentWatches(includeCurrentWatches);
        return this;
    }

    public WatcherStatsRequestBuilder setIncludeQueuedWatches(boolean includeQueuedWatches) {
        request().includeQueuedWatches(includeQueuedWatches);
        return this;
    }
}
