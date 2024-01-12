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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.index.reindex.WorkerBulkByScrollTaskState;
import org.elasticsearch.script.CtxMap;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.UpdateByQueryMetadata;
import org.elasticsearch.script.UpdateByQueryScript;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;

public class TransportUpdateByQueryAction extends HandledTransportAction<UpdateByQueryRequest, BulkByScrollResponse> {

    private final Client client;
    private final ScriptService scriptService;
    private final TransportService transportService;

    @Inject
    public TransportUpdateByQueryAction(
        ActionFilters actionFilters,
        Client client,
        TransportService transportService,
        ScriptService scriptService
    ) {
        super(UpdateByQueryAction.NAME, transportService, actionFilters, UpdateByQueryRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.client = client;
        this.scriptService = scriptService;
        this.transportService = transportService;
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
            transportService.getLocalNode(),
            () -> {
                ParentTaskAssigningClient assigningClient = new ParentTaskAssigningClient(
                    client,
                    transportService.getLocalNode(),
                    bulkByScrollTask
                );
                new AsyncIndexBySearchAction(bulkByScrollTask, logger, assigningClient, scriptService, request, listener).start();
            }
        );
    }

    /**
     * Simple implementation of update-by-query using scrolling and bulk.
     */
    static class AsyncIndexBySearchAction extends AbstractAsyncBulkByScrollAction<UpdateByQueryRequest> {

        AsyncIndexBySearchAction(
            BulkByScrollTask task,
            Logger logger,
            ParentTaskAssigningClient client,
            ScriptService scriptService,
            UpdateByQueryRequest request,
            ActionListener<BulkByScrollResponse> listener
        ) {
            super(
                task,
                // use sequence number powered optimistic concurrency control unless requested
                request.getSearchRequest().source() != null && Boolean.TRUE.equals(request.getSearchRequest().source().version()),
                true,
                logger,
                client,
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
                return new UpdateByQueryScriptApplier(worker, scriptService, script, script.getParams(), threadPool::absoluteTimeInMillis);
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

        static class UpdateByQueryScriptApplier extends ScriptApplier<UpdateByQueryMetadata> {
            private UpdateByQueryScript.Factory update = null;

            UpdateByQueryScriptApplier(
                WorkerBulkByScrollTaskState taskWorker,
                ScriptService scriptService,
                Script script,
                Map<String, Object> params,
                LongSupplier nowInMillisSupplier
            ) {
                super(taskWorker, scriptService, script, params, nowInMillisSupplier);
            }

            @Override
            protected CtxMap<UpdateByQueryMetadata> execute(ScrollableHitSource.Hit doc, Map<String, Object> source) {
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
                update.newInstance(params, ctxMap).execute();
                return ctxMap;
            }

            @Override
            protected void updateRequest(RequestWrapper<?> request, UpdateByQueryMetadata metadata) {
                // do nothing
            }
        }
    }
}
