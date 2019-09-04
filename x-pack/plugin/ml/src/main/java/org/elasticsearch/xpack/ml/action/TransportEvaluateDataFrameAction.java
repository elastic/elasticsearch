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
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.EvaluateDataFrameAction;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.Evaluation;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;

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
        Evaluation evaluation = request.getEvaluation();
        SearchRequest searchRequest = new SearchRequest(request.getIndices());
        searchRequest.source(evaluation.buildSearch(request.getParsedQuery()));

        ActionListener<List<EvaluationMetricResult>> resultsListener = ActionListener.wrap(
            results -> listener.onResponse(new EvaluateDataFrameAction.Response(evaluation.getName(), results)),
            listener::onFailure
        );

        client.execute(SearchAction.INSTANCE, searchRequest, ActionListener.wrap(
            searchResponse -> threadPool.generic().execute(() -> {
                try {
                    evaluation.evaluate(searchResponse, resultsListener);
                } catch (Exception e) {
                    listener.onFailure(e);
                };
            }),
            listener::onFailure
        ));
    }
}
