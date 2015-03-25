/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.stats;

import org.elasticsearch.watcher.client.WatcherAction;
import org.elasticsearch.client.Client;

/**
 * This Action gets the stats for the watcher plugin
 */
public class WatcherStatsAction extends WatcherAction<WatcherStatsRequest, WatcherStatsResponse, WatcherStatsRequestBuilder> {

    public static final WatcherStatsAction INSTANCE = new WatcherStatsAction();
    public static final String NAME = "cluster:monitor/watcher/stats";

    private WatcherStatsAction() {
        super(NAME);
    }

    @Override
    public WatcherStatsResponse newResponse() {
        return new WatcherStatsResponse();
    }

    @Override
    public WatcherStatsRequestBuilder newRequestBuilder(Client client) {
        return new WatcherStatsRequestBuilder(client);
    }

}
