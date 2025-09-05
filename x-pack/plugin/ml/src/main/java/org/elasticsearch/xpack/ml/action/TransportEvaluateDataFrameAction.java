/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.action.EvaluateDataFrameAction;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.Evaluation;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationParameters;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.ml.utils.TypedChainTaskExecutor;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.search.aggregations.MultiBucketConsumerService.MAX_BUCKET_SETTING;
import static org.elasticsearch.xpack.ml.utils.SecondaryAuthorizationUtils.useSecondaryAuthIfAvailable;

public class TransportEvaluateDataFrameAction extends HandledTransportAction<
    EvaluateDataFrameAction.Request,
    EvaluateDataFrameAction.Response> {

    private final ThreadPool threadPool;
    private final Client client;
    private final AtomicReference<Integer> maxBuckets = new AtomicReference<>();
    private final SecurityContext securityContext;
    private final ClusterService clusterService;

    @Inject
    public TransportEvaluateDataFrameAction(
        TransportService transportService,
        Settings settings,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Client client,
        ClusterService clusterService
    ) {
        super(
            EvaluateDataFrameAction.NAME,
            transportService,
            actionFilters,
            EvaluateDataFrameAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.threadPool = threadPool;
        this.client = client;
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings)
            ? new SecurityContext(settings, threadPool.getThreadContext())
            : null;
        this.maxBuckets.set(MAX_BUCKET_SETTING.get(clusterService.getSettings()));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_BUCKET_SETTING, this::setMaxBuckets);
        this.clusterService = clusterService;
    }

    private void setMaxBuckets(int maxBuckets) {
        this.maxBuckets.set(maxBuckets);
    }

    @Override
    protected void doExecute(
        Task task,
        EvaluateDataFrameAction.Request request,
        ActionListener<EvaluateDataFrameAction.Response> listener
    ) {
        TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        ActionListener<List<Void>> resultsListener = listener.delegateFailureAndWrap(
            (delegate, unused) -> delegate.onResponse(
                new EvaluateDataFrameAction.Response(request.getEvaluation().getName(), request.getEvaluation().getResults())
            )
        );

        // Create an immutable collection of parameters to be used by evaluation metrics.
        EvaluationParameters parameters = new EvaluationParameters(maxBuckets.get());
        EvaluationExecutor evaluationExecutor = new EvaluationExecutor(
            threadPool,
            new ParentTaskAssigningClient(client, parentTaskId),
            parameters,
            request,
            securityContext
        );
        evaluationExecutor.execute(resultsListener);
    }

    /**
     * {@link EvaluationExecutor} class allows for serial execution of evaluation steps.
     *
     * Each step consists of the following phases:
     *  1. build search request with aggs requested by individual metrics
     *  2. execute search action with the request built in (1.)
     *  3. make all individual metrics process the search response obtained in (2.)
     *  4. check if all the metrics have their results computed
     *      a) If so, call the final listener and finish
     *      b) Otherwise, add another step to the queue
     *
     * To avoid infinite loop it is essential that every metric *does* compute its result at some point.
     * */
    private static final class EvaluationExecutor extends TypedChainTaskExecutor<Void> {

        private final Client client;
        private final EvaluationParameters parameters;
        private final EvaluateDataFrameAction.Request request;
        private final Evaluation evaluation;
        private final SecurityContext securityContext;

        EvaluationExecutor(
            ThreadPool threadPool,
            Client client,
            EvaluationParameters parameters,
            EvaluateDataFrameAction.Request request,
            SecurityContext securityContext
        ) {
            super(threadPool.generic(), Predicates.always(), Predicates.always());
            this.client = client;
            this.parameters = parameters;
            this.request = request;
            this.evaluation = request.getEvaluation();
            this.securityContext = securityContext;
            // Add one task only. Other tasks will be added as needed by the nextTask method itself.
            add(nextTask());
        }

        private TypedChainTaskExecutor.ChainTask<Void> nextTask() {
            return listener -> {
                SearchSourceBuilder searchSourceBuilder = evaluation.buildSearch(parameters, request.getParsedQuery());
                SearchRequest searchRequest = new SearchRequest(request.getIndices()).source(searchSourceBuilder);
                useSecondaryAuthIfAvailable(
                    securityContext,
                    () -> client.execute(TransportSearchAction.TYPE, searchRequest, listener.delegateFailureAndWrap((l, searchResponse) -> {
                        evaluation.process(searchResponse);
                        if (evaluation.hasAllResults() == false) {
                            add(nextTask());
                        }
                        l.onResponse(null);
                    }))
                );
            };
        }
    }
}
