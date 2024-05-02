/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.GetInferenceStatsAction;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;

import java.util.Objects;

import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

public class TransportGetInferenceStatsAction extends HandledTransportAction<
    GetInferenceStatsAction.Request,
    GetInferenceStatsAction.Response> {

    private final ThreadPool threadPool;
    private final HttpClientManager httpClientManager;

    @Inject
    public TransportGetInferenceStatsAction(
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        HttpClientManager httpClientManager
    ) {
        super(
            GetInferenceStatsAction.NAME,
            transportService,
            actionFilters,
            GetInferenceStatsAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        this.threadPool = Objects.requireNonNull(threadPool);
        this.httpClientManager = Objects.requireNonNull(httpClientManager);
    }

    @Override
    protected void doExecute(
        Task task,
        GetInferenceStatsAction.Request request,
        ActionListener<GetInferenceStatsAction.Response> listener
    ) {
        threadPool.executor(UTILITY_THREAD_POOL_NAME)
            .execute(() -> listener.onResponse(new GetInferenceStatsAction.Response(httpClientManager.getPoolStats())));
    }
}
