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
import org.elasticsearch.xpack.querysampling.storage.GoldenStaleness;
import org.elasticsearch.xpack.querysampling.storage.GoldenUpdater;
import org.elasticsearch.xpack.querysampling.storage.GoldenWriter;
import org.elasticsearch.xpack.querysampling.storage.StoredSamples;

import static org.elasticsearch.xpack.core.ClientHelper.QUERY_SAMPLING_ORIGIN;

/**
 * Brings the golden dataset up to date by a version. The golden dataset and the sample are read and written as the plugin, as nobody
 * else may touch them, while the searches that look at the data and the exact searches go through the node client with the thread
 * context still holding the caller, so they see what the caller is allowed to see and nothing more.
 */
public final class TransportQuerySamplingGoldenUpdateAction extends TransportAction<
    QuerySamplingGoldenUpdateRequest,
    QuerySamplingGoldenUpdateResponse> {

    private final GoldenUpdater updater;

    @Inject
    public TransportQuerySamplingGoldenUpdateAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(QuerySamplingGoldenUpdateAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        OriginSettingClient pluginClient = new OriginSettingClient(client, QUERY_SAMPLING_ORIGIN);
        GoldenReader reader = new GoldenReader(pluginClient::search, xContentRegistry);
        this.updater = new GoldenUpdater(
            reader,
            new StoredSamples(pluginClient::search, xContentRegistry),
            new GoldenWriter(
                pluginClient::search,
                pluginClient::index,
                pluginClient::bulk,
                pluginClient::update,
                threadPool::absoluteTimeInMillis
            ),
            new GoldenStaleness(reader, client::search),
            client::search
        );
    }

    @Override
    protected void doExecute(
        Task task,
        QuerySamplingGoldenUpdateRequest request,
        ActionListener<QuerySamplingGoldenUpdateResponse> listener
    ) {
        updater.update(request.max(), listener.map(QuerySamplingGoldenUpdateResponse::new));
    }
}
