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
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.querysampling.storage.GoldenReader;
import org.elasticsearch.xpack.querysampling.storage.GoldenStaleness;

import static org.elasticsearch.xpack.core.ClientHelper.QUERY_SAMPLING_ORIGIN;

/**
 * Checks the queries of a version of the golden dataset for ground truth that is out of date. The golden dataset is read as
 * the plugin, as nobody else may touch it, while the searches that look at the data go through the node client with the
 * thread context still holding the caller, so they see what the caller is allowed to see and nothing more.
 */
public final class TransportQuerySamplingGoldenCheckAction extends TransportAction<
    QuerySamplingGoldenCheckRequest,
    QuerySamplingGoldenCheckResponse> {

    private final GoldenStaleness staleness;

    @Inject
    public TransportQuerySamplingGoldenCheckAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(QuerySamplingGoldenCheckAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        OriginSettingClient pluginClient = new OriginSettingClient(client, QUERY_SAMPLING_ORIGIN);
        this.staleness = new GoldenStaleness(new GoldenReader(pluginClient::search, xContentRegistry), client::search);
    }

    @Override
    protected void doExecute(
        Task task,
        QuerySamplingGoldenCheckRequest request,
        ActionListener<QuerySamplingGoldenCheckResponse> listener
    ) {
        staleness.check(request.version(), request.max(), listener.map(QuerySamplingGoldenCheckResponse::new));
    }
}
