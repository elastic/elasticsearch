/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.DenseSearchAction;
import org.elasticsearch.xpack.core.ml.action.InferTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.results.TextEmbeddingResults;

public class TransportDenseSearchAction extends HandledTransportAction<DenseSearchAction.Request, DenseSearchAction.Response> {

    private final Client client;
    private final ClusterService clusterService;

    @Inject
    public TransportDenseSearchAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService
    ) {
        super(DenseSearchAction.NAME, transportService, actionFilters, DenseSearchAction.Request::new);
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, DenseSearchAction.Request request, ActionListener<DenseSearchAction.Response> listener) {

        var parentTaskAssigningClient = new ParentTaskAssigningClient(client, clusterService.localNode(), task);

        if (request.getKnnSearchBuilder() == null) {
            listener.onFailure(
                new IllegalArgumentException(
                    "missing required [" + DenseSearchAction.Request.KNN.getPreferredName() + "] section in search body"
                )
            );
            return;
        }

        parentTaskAssigningClient.execute(
            InferTrainedModelDeploymentAction.INSTANCE,
            toInferenceRequest(request),
            ActionListener.wrap(inferenceResults -> {
                if (inferenceResults.getResults()instanceof TextEmbeddingResults textEmbeddingResults) {
                    var searchRequestBuilder = buildSearch(parentTaskAssigningClient, textEmbeddingResults, request);

                    searchRequestBuilder.execute(ActionListener.wrap(searchResponse -> {
                        listener.onResponse(
                            new DenseSearchAction.Response(
                                searchResponse.getTook(),
                                TimeValue.timeValueMillis(inferenceResults.getTookMillis()),
                                searchResponse
                            )
                        );
                    }, listener::onFailure));
                } else {
                    listener.onFailure(
                        new IllegalArgumentException(
                            "model ["
                                + request.getDeploymentId()
                                + "] must be a text_embedding model; provided ["
                                + inferenceResults.getResults().getWriteableName()
                                + "]"
                        )
                    );
                }
            }, listener::onFailure)
        );
    }

    private SearchRequestBuilder buildSearch(
        ParentTaskAssigningClient client,
        TextEmbeddingResults inferenceResults,
        DenseSearchAction.Request request
    ) {
        var searchBuilder = client.unwrap().prepareSearch();
        searchBuilder.setIndices(request.getIndices());
        if (request.getRouting() != null) {
            searchBuilder.setRouting(request.getRouting());
        }

        var knnBuilder = request.getKnnSearchBuilder();
        knnBuilder.queryVector(inferenceResults.getInferenceAsFloat());

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.trackTotalHitsUpTo(SearchContext.TRACK_TOTAL_HITS_ACCURATE);
        sourceBuilder.knnSearch(knnBuilder);
        sourceBuilder.size(knnBuilder.k());

        if (request.getFetchSource() != null) {
            sourceBuilder.fetchSource(request.getFetchSource());
        }
        if (request.getFields() != null) {
            for (FieldAndFormat field : request.getFields()) {
                sourceBuilder.fetchField(field);
            }
        }
        if (request.getDocValueFields() != null) {
            for (FieldAndFormat field : request.getDocValueFields()) {
                sourceBuilder.docValueField(field.field, field.format);
            }
        }

        searchBuilder.setSource(sourceBuilder);
        return searchBuilder;
    }

    private InferTrainedModelDeploymentAction.Request toInferenceRequest(DenseSearchAction.Request request) {
        return new InferTrainedModelDeploymentAction.Request(
            request.getDeploymentId(),
            request.getEmbeddingConfig(),
            request.getQueryString(),
            request.getInferenceTimeout()
        );
    }
}
