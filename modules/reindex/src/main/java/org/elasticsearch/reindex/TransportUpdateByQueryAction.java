/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchResponse;
import org.elasticsearch.index.reindex.BulkByPaginatedSearchTask;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.index.reindex.WorkerBulkByPaginatedSearchTaskState;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.node.ShutdownPrepareService;
import org.elasticsearch.script.CtxMap;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.UpdateByQueryMetadata;
import org.elasticsearch.script.UpdateByQueryScript;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;

public class TransportUpdateByQueryAction extends HandledTransportAction<UpdateByQueryRequest, BulkByPaginatedSearchResponse> {

    private final ThreadPool threadPool;
    private final Client client;
    private final ScriptService scriptService;
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final UpdateByQueryMetrics updateByQueryMetrics;
    @Nullable
    private final BulkByPaginatedSearchSearchContextMetrics bulkByPaginatedSearchSearchContextMetrics;
    private final TimeValue taskShutdownGracePeriod;
    private final ReindexSettings reindexSettings;
    private final CircuitBreaker requestBreaker;

    @Inject
    public TransportUpdateByQueryAction(
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client,
        TransportService transportService,
        ScriptService scriptService,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        @Nullable UpdateByQueryMetrics updateByQueryMetrics,
        @Nullable BulkByPaginatedSearchSearchContextMetrics bulkByPaginatedSearchSearchContextMetrics,
        ReindexSettings reindexSettings,
        CircuitBreakerService circuitBreakerService
    ) {
        super(UpdateByQueryAction.NAME, transportService, actionFilters, UpdateByQueryRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.threadPool = threadPool;
        this.client = client;
        this.scriptService = scriptService;
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.updateByQueryMetrics = updateByQueryMetrics;
        this.bulkByPaginatedSearchSearchContextMetrics = bulkByPaginatedSearchSearchContextMetrics;
        // todo: if relocations are added to update-by-query and it gets its own timeout setting, this should be updated.
        // without this safe default, adding relocations to update-by-query without updating this might open it up to race conditions.
        this.taskShutdownGracePeriod = ShutdownPrepareService.MAXIMUM_REINDEXING_TIMEOUT_SETTING.get(clusterService.getSettings());
        this.reindexSettings = Objects.requireNonNull(reindexSettings);
        this.requestBreaker = Objects.requireNonNull(circuitBreakerService.getBreaker(CircuitBreaker.REQUEST));
    }

    @Override
    protected void doExecute(Task task, UpdateByQueryRequest request, ActionListener<BulkByPaginatedSearchResponse> listener) {
        BulkByPaginatedSearchTask bulkByPaginatedSearchTask = (BulkByPaginatedSearchTask) task;
        long startTime = System.nanoTime();
        ProjectMetadata projectMetadata = projectResolver.getProjectState(clusterService.state()).metadata();
        BulkByPaginatedSearchParallelizationHelper.validateSliceRoutingForWriteBackedSearch(
            request,
            projectMetadata,
            "update by query request"
        );
        BulkByPaginatedSearchParallelizationHelper.startSlicedAction(
            request,
            bulkByPaginatedSearchTask,
            UpdateByQueryAction.INSTANCE,
            listener,
            client,
            clusterService.localNode(),
            () -> {
                ParentTaskAssigningClient assigningClient = new ParentTaskAssigningClient(
                    client,
                    clusterService.localNode(),
                    bulkByPaginatedSearchTask
                );
                new AsyncIndexBySearchAction(
                    bulkByPaginatedSearchTask,
                    logger,
                    assigningClient,
                    threadPool,
                    scriptService,
                    request,
                    ActionListener.runAfter(listener, () -> {
                        long elapsedTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime);
                        if (updateByQueryMetrics != null) {
                            updateByQueryMetrics.recordTookTime(elapsedTime);
                        }
                    }),
                    taskShutdownGracePeriod,
                    bulkByPaginatedSearchSearchContextMetrics,
                    reindexSettings,
                    requestBreaker
                ).start();
            }
        );
    }

    /**
     * Simple implementation of update-by-query using scrolling and bulk.
     */
    static class AsyncIndexBySearchAction extends AbstractAsyncBulkByPaginatedSearchAction<
        UpdateByQueryRequest,
        TransportUpdateByQueryAction> {

        AsyncIndexBySearchAction(
            BulkByPaginatedSearchTask task,
            Logger logger,
            ParentTaskAssigningClient client,
            ThreadPool threadPool,
            ScriptService scriptService,
            UpdateByQueryRequest request,
            ActionListener<BulkByPaginatedSearchResponse> listener,
            TimeValue maxTaskShutdownGracePeriod,
            @Nullable BulkByPaginatedSearchSearchContextMetrics bulkByPaginatedSearchSearchContextMetrics,
            ReindexSettings reindexSettings,
            CircuitBreaker requestBreaker
        ) {
            super(
                task,
                // use sequence number powered optimistic concurrency control unless requested
                request.getSearchRequest().source() != null && Boolean.TRUE.equals(request.getSearchRequest().source().version()),
                true,
                true,
                logger,
                client,
                threadPool,
                request,
                listener,
                scriptService,
                null,
                bulkByPaginatedSearchSearchContextMetrics,
                BulkByPaginatedSearchSearchContextMetrics.TaskKind.UPDATE_BY_QUERY,
                false,
                maxTaskShutdownGracePeriod,
                reindexSettings,
                requestBreaker,
                "update_by_query_bulk_batch"
            );
        }

        @Override
        public BiFunction<RequestWrapper<?>, PaginatedHitSource.Hit, RequestWrapper<?>> buildScriptApplier() {
            Script script = mainRequest.getScript();
            if (script != null) {
                return new UpdateByQueryScriptApplier(worker, scriptService, script, script.getParams(), threadPool::absoluteTimeInMillis);
            }
            return super.buildScriptApplier();
        }

        @Override
        protected RequestWrapper<IndexRequest> buildRequest(PaginatedHitSource.Hit doc) {
            IndexRequest index = new IndexRequest();
            index.index(doc.getIndex());
            index.id(doc.getId());
            index.source(doc.getSource(), doc.getXContentType());
            index.setIfSeqNo(doc.getSeqNo());
            index.setIfPrimaryTerm(doc.getPrimaryTerm());
            index.setPipeline(mainRequest.getPipeline());
            return wrap(index);
        }

        static class UpdateByQueryScriptApplier extends ScriptApplier<UpdateByQueryMetadata> {
            private UpdateByQueryScript.Factory update = null;

            UpdateByQueryScriptApplier(
                WorkerBulkByPaginatedSearchTaskState taskWorker,
                ScriptService scriptService,
                Script script,
                Map<String, Object> params,
                LongSupplier nowInMillisSupplier
            ) {
                super(taskWorker, scriptService, script, params, nowInMillisSupplier);
            }

            @Override
            protected CtxMap<UpdateByQueryMetadata> execute(PaginatedHitSource.Hit doc, Map<String, Object> source) {
                if (update == null) {
                    update = scriptService.compile(script, UpdateByQueryScript.CONTEXT);
                }
                CtxMap<UpdateByQueryMetadata> ctxMap = new CtxMap<>(
                    source,
                    new UpdateByQueryMetadata(
                        doc.getIndex(),
                        doc.getId(),
                        doc.getVersion(),
                        doc.getRouting(),
                        INDEX,
                        nowInMillisSupplier.getAsLong()
                    )
                );
                UpdateByQueryScript instance = update.newInstance(params, ctxMap);
                instance._setCancellationCheck(buildCancellationCheck());
                instance.execute();
                return ctxMap;
            }

            @Override
            protected void updateRequest(RequestWrapper<?> request, UpdateByQueryMetadata metadata) {
                // do nothing
            }
        }
    }
}
