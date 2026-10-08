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
import org.elasticsearch.xpack.querysampling.groundtruth.StoredGroundTruth;

import static org.elasticsearch.xpack.core.ClientHelper.QUERY_SAMPLING_ORIGIN;

/**
 * Computes the ground truth of the stored sampled queries that do not have it. The node that receives the request
 * does the work, whichever node picked the queries. The exact searches go through the node client while the
 * thread context still holds the caller, so they see what the caller is allowed to see and nothing more, while
 * the index of the sample is read and updated as the plugin.
 */
public final class TransportQuerySamplingGroundTruthAction extends TransportAction<
    QuerySamplingGroundTruthRequest,
    QuerySamplingGroundTruthResponse> {

    private final StoredGroundTruth storedGroundTruth;

    @Inject
    public TransportQuerySamplingGroundTruthAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(QuerySamplingGroundTruthAction.NAME, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        OriginSettingClient sampleClient = new OriginSettingClient(client, QUERY_SAMPLING_ORIGIN);
        this.storedGroundTruth = new StoredGroundTruth(
            sampleClient::search,
            sampleClient::bulk,
            client::search,
            xContentRegistry,
            threadPool::absoluteTimeInMillis
        );
    }

    @Override
    protected void doExecute(
        Task task,
        QuerySamplingGroundTruthRequest request,
        ActionListener<QuerySamplingGroundTruthResponse> listener
    ) {
        storedGroundTruth.compute(
            request.max(),
            listener.map(result -> new QuerySamplingGroundTruthResponse(result.computed(), result.failed()))
        );
    }
}
