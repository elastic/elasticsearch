/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.rollup.v2.RollupV2Action;
import org.elasticsearch.xpack.core.rollup.v2.RollupV2Task;
import org.elasticsearch.xpack.rollup.Rollup;

public class TransportRollupV2Action extends HandledTransportAction<RollupV2Action.Request, RollupV2Action.Response> {
    private final Client client;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;

    @Inject
    public TransportRollupV2Action(
            final Client client,
            final ClusterService clusterService,
            final ThreadPool threadPool,
            final TransportService transportService,
            final ActionFilters actionFilters
    ) {
        super(RollupV2Action.NAME, transportService, actionFilters, RollupV2Action.Request::new);
        this.client = client;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, RollupV2Action.Request request, ActionListener<RollupV2Action.Response> listener) {
        RollupV2Task rollupV2Task = (RollupV2Task) task;
        RollupV2Indexer indexer = new RollupV2Indexer(client, threadPool, Rollup.TASK_THREAD_POOL_NAME,
            rollupV2Task.config(), rollupV2Task.headers(), ActionListener.wrap(c -> {
                listener.onResponse(new RollupV2Action.Response(true));
        }, e -> listener.onFailure(
            new ElasticsearchException("Failed to rollup index [" + rollupV2Task.config().getSourceIndex() + "]", e))));
        if (indexer.start() == IndexerState.STARTED) {
            indexer.maybeTriggerAsyncJob(Long.MAX_VALUE);
        } else {
            listener.onFailure(new ElasticsearchException("failed to start rollup task"));
        }
    }
}
