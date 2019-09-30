/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.EvaluateDataFrameAction;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.Evaluation;
import org.elasticsearch.xpack.ml.utils.TypedChainTaskExecutor;

import java.util.List;

public class TransportEvaluateDataFrameAction extends HandledTransportAction<EvaluateDataFrameAction.Request,
    EvaluateDataFrameAction.Response> {

    private final ThreadPool threadPool;
    private final Client client;

    @Inject
    public TransportEvaluateDataFrameAction(TransportService transportService, ActionFilters actionFilters, ThreadPool threadPool,
                                            Client client) {
        super(EvaluateDataFrameAction.NAME, transportService, actionFilters, EvaluateDataFrameAction.Request::new);
        this.threadPool = threadPool;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, EvaluateDataFrameAction.Request request,
                             ActionListener<EvaluateDataFrameAction.Response> listener) {
        ActionListener<List<Void>> resultsListener = ActionListener.wrap(
            unused -> {
                EvaluateDataFrameAction.Response response =
                    new EvaluateDataFrameAction.Response(request.getEvaluation().getName(), request.getEvaluation().getResults());
                listener.onResponse(response);
            },
            listener::onFailure
        );

        EvaluationExecutor evaluationExecutor = new EvaluationExecutor(threadPool, client, request);
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
        private final EvaluateDataFrameAction.Request request;
        private final Evaluation evaluation;

        EvaluationExecutor(ThreadPool threadPool, Client client, EvaluateDataFrameAction.Request request) {
            super(threadPool.generic(), unused -> true, unused -> true);
            this.client = client;
            this.request = request;
            this.evaluation = request.getEvaluation();
            // Add one task only. Other tasks will be added as needed by the nextTask method itself.
            add(nextTask());
        }

        private TypedChainTaskExecutor.ChainTask<Void> nextTask() {
            return listener -> {
                SearchSourceBuilder searchSourceBuilder = evaluation.buildSearch(request.getParsedQuery());
                SearchRequest searchRequest = new SearchRequest(request.getIndices()).source(searchSourceBuilder);
                client.execute(
                    SearchAction.INSTANCE,
                    searchRequest,
                    ActionListener.wrap(
                        searchResponse -> {
                            evaluation.process(searchResponse);
                            if (evaluation.hasAllResults() == false) {
                                add(nextTask());
                            }
                            listener.onResponse(null);
                        },
                        listener::onFailure));
            };
        }
    }
}
