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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;

public class TransportUpdateByQueryAction extends HandledTransportAction<UpdateByQueryRequest, BulkByScrollResponse> {

    private final ThreadPool threadPool;
    private final Client client;
    private final ScriptService scriptService;
    private final ClusterService clusterService;
    private final UpdateByQueryMetrics updateByQueryMetrics;

    public TransportUpdateByQueryAction(
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Client client,
        TransportService transportService,
        ScriptService scriptService,
        ClusterService clusterService,
        @Nullable UpdateByQueryMetrics updateByQueryMetrics
    ) {
        super(UpdateByQueryAction.NAME, transportService, actionFilters, UpdateByQueryRequest::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.threadPool = threadPool;
        this.client = client;
        this.scriptService = scriptService;
        this.clusterService = clusterService;
        this.updateByQueryMetrics = updateByQueryMetrics;
    }

    @Override
    protected void doExecute(Task task, UpdateByQueryRequest request, ActionListener<BulkByScrollResponse> listener) {
        // ✅ Convert doc to script if needed
        if (request.getScript() == null && request.getDoc() != null) {
            StringBuilder scriptBuilder = new StringBuilder();
            for (Map.Entry<String, Object> entry : request.getDoc().entrySet()) {
                scriptBuilder.append("ctx._source.")
                             .append(entry.getKey())
                             .append(" = params.")
                             .append(entry.getKey())
                             .append("; ");
            }
            Script generatedScript = new Script(
                Script.DEFAULT_SCRIPT_TYPE,
                Script.DEFAULT_SCRIPT_LANG,
                scriptBuilder.toString().trim(),
                request.getDoc()
            );
            request.setScript(generatedScript);
        }

        BulkByScrollTask bulkByScrollTask = (BulkByScrollTask) task;
        long startTime = System.nanoTime();
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
                new AsyncIndexBySearchAction(
                    bulkByScrollTask,
                    logger,
                    assigningClient,
                    threadPool,
                    scriptService,
                    request,
                    state,
                    ActionListener.runAfter(listener, () -> {
                        long elapsedTime = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime);
                        if (updateByQueryMetrics != null) {
                            updateByQueryMetrics.recordTookTime(elapsedTime);
                        }
                    })
                ).start();
            }
        );
    }

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
                request.getSearchRequest().source() != null && Boolean.TRUE.equals(request.getSearchRequest().source().version()),
                true,
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
                // no-op
            }
        }
    }
}
