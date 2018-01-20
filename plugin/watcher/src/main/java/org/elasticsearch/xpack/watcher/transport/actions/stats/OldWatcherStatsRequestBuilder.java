/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.stats;

import org.elasticsearch.action.support.master.MasterNodeReadOperationRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Watcher stats request builder.
 */
public class OldWatcherStatsRequestBuilder extends MasterNodeReadOperationRequestBuilder<OldWatcherStatsRequest, OldWatcherStatsResponse,
        OldWatcherStatsRequestBuilder> {

    public OldWatcherStatsRequestBuilder(ElasticsearchClient client) {
        super(client, OldWatcherStatsAction.INSTANCE, new OldWatcherStatsRequest());
    }

    public OldWatcherStatsRequestBuilder setIncludeCurrentWatches(boolean includeCurrentWatches) {
        request().includeCurrentWatches(includeCurrentWatches);
        return this;
    }

    public OldWatcherStatsRequestBuilder setIncludeQueuedWatches(boolean includeQueuedWatches) {
        request().includeQueuedWatches(includeQueuedWatches);
        return this;
    }
}
