/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.querysampling.storage.GoldenReader;
import org.elasticsearch.xpack.querysampling.storage.GoldenRefresher;
import org.elasticsearch.xpack.querysampling.storage.GoldenStaleness;
import org.elasticsearch.xpack.querysampling.storage.GoldenWriter;

import static org.elasticsearch.xpack.core.ClientHelper.QUERY_SAMPLING_ORIGIN;

/**
 * Makes a new version of the golden dataset in which the ground truth of the stale queries is computed again. The golden
 * dataset is read and written as the plugin, as nobody else may touch it, while the searches that look at the data and the exact
 * searches go through the node client with the thread context still holding the caller, so they see what the caller is allowed
 * to see and nothing more.
 */
public final class TransportQuerySamplingGoldenRefreshAction extends TransportAction<
    QuerySamplingGoldenRefreshRequest,
    QuerySamplingGoldenRefreshResponse> {

    private final GoldenRefresher refresher;

    @Inject
    public TransportQuerySamplingGoldenRefreshAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(QuerySamplingGoldenRefreshAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        OriginSettingClient pluginClient = new OriginSettingClient(client, QUERY_SAMPLING_ORIGIN);
        GoldenReader reader = new GoldenReader(pluginClient::search, xContentRegistry);
        this.refresher = new GoldenRefresher(
            reader,
            new GoldenStaleness(reader, client::search),
            client::search,
            new GoldenWriter(
                pluginClient::search,
                pluginClient::index,
                pluginClient::bulk,
                pluginClient::update,
                threadPool::absoluteTimeInMillis
            )
        );
    }

    @Override
    protected void doExecute(
        Task task,
        QuerySamplingGoldenRefreshRequest request,
        ActionListener<QuerySamplingGoldenRefreshResponse> listener
    ) {
        refresher.refresh(request.version(), request.max(), listener.map(QuerySamplingGoldenRefreshResponse::new));
    }
}
