/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.stats;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * This exists only for BWC against older 5.x nodes, which do not gather stats in a distributed fashion to support rolling upgrades
 */
public class OldWatcherStatsAction extends Action<OldWatcherStatsRequest, OldWatcherStatsResponse, OldWatcherStatsRequestBuilder> {

    public static final OldWatcherStatsAction INSTANCE = new OldWatcherStatsAction();
    public static final String NAME = "cluster:monitor/xpack/watcher/stats";

    private OldWatcherStatsAction() {
        super(NAME);
    }

    @Override
    public OldWatcherStatsResponse newResponse() {
        return new OldWatcherStatsResponse();
    }

    @Override
    public OldWatcherStatsRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new OldWatcherStatsRequestBuilder(client);
    }
}
