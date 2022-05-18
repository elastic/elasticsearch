/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.index.reindex.WorkerBulkByScrollTaskState;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.field.Op;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.function.BiFunction;

public class TransportUpdateByQueryAction extends HandledTransportAction<UpdateByQueryRequest, BulkByScrollResponse> {

    private final ThreadPool threadPool;
    private final Client client;
    private final ScriptService scriptService;
    private final ClusterService clusterService;

    @Inject
    public TransportUpdateByQueryAction(
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client,
        TransportService transportService,
        ScriptService scriptService,
        ClusterService clusterService
    ) {
        super(UpdateByQueryAction.NAME, transportService, actionFilters, UpdateByQueryRequest::new);
        this.threadPool = threadPool;
        this.client = client;
        this.scriptService = scriptService;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, UpdateByQueryRequest request, ActionListener<BulkByScrollResponse> listener) {
        BulkByScrollTask bulkByScrollTask = (BulkByScrollTask) task;
        BulkByScrollParallelizationHelper.startSlicedAction(
            request,
            bulkByScrollTask,
            UpdateByQueryAction.INSTANCE,
            listener,
            client,
            clusterService.localNode(),
            () -> {
                ClusterState state = clusterService.state();
                ParentTaskAssigningClient assigningClient = new ParentTaskAssigningClient(
                    client,
                    clusterService.localNode(),
                    bulkByScrollTask
                );
                new AsyncIndexBySearchAction(bulkByScrollTask, logger, assigningClient, threadPool, scriptService, request, state, listener)
                    .start();
            }
        );
    }

    /**
     * Simple implementation of update-by-query using scrolling and bulk.
     */
    static class AsyncIndexBySearchAction extends AbstractAsyncBulkByScrollAction<UpdateByQueryRequest, TransportUpdateByQueryAction> {

        AsyncIndexBySearchAction(
            BulkByScrollTask task,
            Logger logger,
            ParentTaskAssigningClient client,
            ThreadPool threadPool,
            ScriptService scriptService,
            UpdateByQueryRequest request,
            ClusterState clusterState,
            ActionListener<BulkByScrollResponse> listener
        ) {
            super(
                task,
                // use sequence number powered optimistic concurrency control
                false,
                true,
                logger,
                client,
                threadPool,
                request,
                listener,
                scriptService,
                null
            );
        }

        @Override
        public BiFunction<RequestWrapper<?>, ScrollableHitSource.Hit, RequestWrapper<?>> buildScriptApplier() {
            Script script = mainRequest.getScript();
            if (script != null) {
                return new UpdateByQueryScriptApplier(worker, scriptService, script, script.getParams());
            }
            return super.buildScriptApplier();
        }

        @Override
        protected RequestWrapper<IndexRequest> buildRequest(ScrollableHitSource.Hit doc) {
            IndexRequest index = new IndexRequest();
            index.index(doc.getIndex());
            index.id(doc.getId());
            index.source(doc.getSource(), doc.getXContentType());
            index.setIfSeqNo(doc.getSeqNo());
            index.setIfPrimaryTerm(doc.getPrimaryTerm());
            index.setPipeline(mainRequest.getPipeline());
            return wrap(index);
        }

        class UpdateByQueryScriptApplier extends ScriptApplier {

            UpdateByQueryScriptApplier(
                WorkerBulkByScrollTaskState taskWorker,
                ScriptService scriptService,
                Script script,
                Map<String, Object> params
            ) {
                super(taskWorker, scriptService, script, params);
            }

            @Override
            protected void scriptChangedIndex(RequestWrapper<?> request, String index) {
                // TODO(stu): this is handled in validate
                throw new IllegalArgumentException("Modifying [" + IndexFieldMapper.NAME + "] not allowed");
            }

            @Override
            protected void scriptChangedId(RequestWrapper<?> request, String id) {
                // TODO(stu): this is handled in validate
                throw new IllegalArgumentException("Modifying [" + IdFieldMapper.NAME + "] not allowed");
            }

            @Override
            protected void scriptChangedVersion(RequestWrapper<?> request, Long version) {
                // TODO(stu): this is handled in validate
                throw new IllegalArgumentException("Modifying [_version] not allowed");
            }

            @Override
            protected void scriptChangedRouting(RequestWrapper<?> request, String routing) {
                // TODO(stu): this is handled in validate
                throw new IllegalArgumentException("Modifying [" + RoutingFieldMapper.NAME + "] not allowed");
            }

            @Override
            protected AbstractReindexMetadata metadata(
                Map<String, Object> context,
                String index,
                String id,
                String routing,
                Long version,
                Op op
            ) {
                return new UpdateByQueryMetadata(context, index, id, routing, version, op);
            }
        }

        static class UpdateByQueryMetadata extends AbstractReindexMetadata {
            UpdateByQueryMetadata(Map<String, Object> ctx, String index, String id, String routing, Long version, Op op) {
                super(ctx, index, id, routing, version, op);
            }

            @Override
            public void validate() {
                if (opChanged()) {
                    validateOp(getOp());
                }
                if (indexChanged()) {
                    unsupported(INDEX_NAME, true);
                }
                if (idChanged()) {
                    unsupported(ID_NAME, true);
                }
                if (routingChanged()) {
                    unsupported(ROUTING_NAME, true);
                }
                if (versionChanged()) {
                    unsupported(VERSION_NAME, true);
                }
            }

            @Override
            public void setIndex(String index) {
                unsupported(INDEX_NAME, true);
            }

            @Override
            public void setId(String id) {
                unsupported(ID_NAME, true);
            }

            @Override
            public void setRouting(String routing) {
                unsupported(ROUTING_NAME, true);
            }

            @Override
            public void setVersion(Long version) {
                unsupported(VERSION_NAME, true);
            }
        }
    }
}
