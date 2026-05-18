/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchTask;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.ShutdownPrepareService;
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
    @Nullable
    private final BulkByScrollSearchContextMetrics bulkByScrollSearchContextMetrics;
    private final TimeValue taskShutdownGracePeriod;

    @Inject
    public TransportDeleteByQueryAction(
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client,
        TransportService transportService,
        ScriptService scriptService,
        ClusterService clusterService,
        @Nullable DeleteByQueryMetrics deleteByQueryMetrics,
        @Nullable BulkByScrollSearchContextMetrics bulkByScrollSearchContextMetrics
    ) {
        super(DeleteByQueryAction.NAME, transportService, actionFilters, DeleteByQueryRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.threadPool = threadPool;
        this.client = client;
        this.scriptService = scriptService;
        this.clusterService = clusterService;
        this.deleteByQueryMetrics = deleteByQueryMetrics;
        this.bulkByScrollSearchContextMetrics = bulkByScrollSearchContextMetrics;
        // todo: if relocations are added to delete-by-query and it gets its own timeout setting, this should be updated.
        // without this safe default, adding relocations to delete-by-query without updating this might open it up to race conditions.
        this.taskShutdownGracePeriod = ShutdownPrepareService.MAXIMUM_REINDEXING_TIMEOUT_SETTING.get(clusterService.getSettings());
    }

    @Override
    public void doExecute(Task task, DeleteByQueryRequest request, ActionListener<BulkByScrollResponse> listener) {
        BulkByPaginatedSearchTask bulkByPaginatedSearchTask = (BulkByPaginatedSearchTask) task;
        long startTime = System.nanoTime();
        BulkByPaginatedSearchParallelizationHelper.startSlicedAction(
            request,
            bulkByPaginatedSearchTask,
            DeleteByQueryAction.INSTANCE,
            listener,
            client,
            clusterService.localNode(),
            () -> {
                ParentTaskAssigningClient assigningClient = new ParentTaskAssigningClient(
                    client,
                    clusterService.localNode(),
                    bulkByPaginatedSearchTask
                );
                new AsyncDeleteByQueryAction(
                    bulkByPaginatedSearchTask,
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
                    }),
                    bulkByScrollSearchContextMetrics,
                    taskShutdownGracePeriod
                ).start();
            }
        );
    }
}
