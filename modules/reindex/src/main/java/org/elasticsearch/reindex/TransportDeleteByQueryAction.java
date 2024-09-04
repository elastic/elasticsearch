/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.TimeUnit;

public class TransportDeleteByQueryAction extends HandledTransportAction<DeleteByQueryRequest, BulkByScrollResponse> {

    private final ThreadPool threadPool;
    private final Client client;
    private final ScriptService scriptService;
    private final ClusterService clusterService;
    private final DeleteByQueryMetrics deleteByQueryMetrics;

    @Inject
    public TransportDeleteByQueryAction(
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client,
        TransportService transportService,
        ScriptService scriptService,
        ClusterService clusterService,
        @Nullable DeleteByQueryMetrics deleteByQueryMetrics
    ) {
        super(DeleteByQueryAction.NAME, transportService, actionFilters, DeleteByQueryRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.threadPool = threadPool;
        this.client = client;
        this.scriptService = scriptService;
        this.clusterService = clusterService;
        this.deleteByQueryMetrics = deleteByQueryMetrics;
    }

    @Override
    public void doExecute(Task task, DeleteByQueryRequest request, ActionListener<BulkByScrollResponse> listener) {
        BulkByScrollTask bulkByScrollTask = (BulkByScrollTask) task;
        long startTime = System.nanoTime();
        BulkByScrollParallelizationHelper.startSlicedAction(
            request,
            bulkByScrollTask,
            DeleteByQueryAction.INSTANCE,
            listener,
            client,
            clusterService.localNode(),
            () -> {
                ParentTaskAssigningClient assigningClient = new ParentTaskAssigningClient(
                    client,
                    clusterService.localNode(),
                    bulkByScrollTask
                );
                new AsyncDeleteByQueryAction(
                    bulkByScrollTask,
                    logger,
                    assigningClient,
                    threadPool,
                    request,
                    scriptService,
                    ActionListener.runAfter(listener, () -> {
                        long elapsedTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime);
                        if (deleteByQueryMetrics != null) {
                            deleteByQueryMetrics.recordTookTime(elapsedTime);
                        }
                    })
                ).start();
            }
        );
    }
}
