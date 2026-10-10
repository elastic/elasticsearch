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
import org.elasticsearch.xpack.querysampling.storage.GoldenPromoter;

import static org.elasticsearch.xpack.core.ClientHelper.QUERY_SAMPLING_ORIGIN;

/**
 * Promotes stored sampled queries to a new version of the golden dataset. The node that receives the request does the work.
 * Both indices are read and written as the plugin, as nobody else may touch them, and no data of the users is read.
 */
public final class TransportQuerySamplingGoldenPromoteAction extends TransportAction<
    QuerySamplingGoldenPromoteRequest,
    QuerySamplingGoldenPromoteResponse> {

    private final GoldenPromoter promoter;

    @Inject
    public TransportQuerySamplingGoldenPromoteAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(QuerySamplingGoldenPromoteAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        OriginSettingClient pluginClient = new OriginSettingClient(client, QUERY_SAMPLING_ORIGIN);
        this.promoter = new GoldenPromoter(
            pluginClient::search,
            pluginClient::index,
            pluginClient::bulk,
            pluginClient::update,
            xContentRegistry,
            threadPool::absoluteTimeInMillis
        );
    }

    @Override
    protected void doExecute(
        Task task,
        QuerySamplingGoldenPromoteRequest request,
        ActionListener<QuerySamplingGoldenPromoteResponse> listener
    ) {
        promoter.promote(
            request.max(),
            listener.map(
                result -> new QuerySamplingGoldenPromoteResponse(
                    result.version() == 0 ? null : result.version(),
                    result.promoted(),
                    result.failed()
                )
            )
        );
    }
}
