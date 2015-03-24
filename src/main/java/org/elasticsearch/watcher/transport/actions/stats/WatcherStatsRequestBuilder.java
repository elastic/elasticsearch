/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.stats;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.watcher.client.WatcherClient;
import org.elasticsearch.client.Client;

/**
 * Watcher stats request builder.
 */
public class WatcherStatsRequestBuilder extends MasterNodeOperationRequestBuilder<WatcherStatsRequest, WatcherStatsResponse, WatcherStatsRequestBuilder, Client> {

    public WatcherStatsRequestBuilder(Client client) {
        super(client, new WatcherStatsRequest());
    }


    @Override
    protected void doExecute(final ActionListener<WatcherStatsResponse> listener) {
        new WatcherClient(client).watcherStats(request, listener);
    }

}
