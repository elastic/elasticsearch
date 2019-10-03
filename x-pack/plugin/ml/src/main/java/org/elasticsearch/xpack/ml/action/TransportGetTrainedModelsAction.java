/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.AbstractTransportGetResourcesAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction.Request;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction.Response;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.persistence.InferenceIndexConstants;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class TransportGetTrainedModelsAction extends AbstractTransportGetResourcesAction<TrainedModelConfig, Request, Response> {

    @Inject
    public TransportGetTrainedModelsAction(TransportService transportService, ActionFilters actionFilters, Client client,
                                           NamedXContentRegistry xContentRegistry) {
        super(GetTrainedModelsAction.NAME, transportService, actionFilters, GetTrainedModelsAction.Request::new, client,
            xContentRegistry);
    }

    @Override
    protected ParseField getResultsField() {
        return GetTrainedModelsAction.Response.RESULTS_FIELD;
    }

    @Override
    protected String[] getIndices() {
        return new String[] {InferenceIndexConstants.INDEX_PATTERN };
    }

    @Override
    protected TrainedModelConfig parse(XContentParser parser) {
        return TrainedModelConfig.LENIENT_PARSER.apply(parser, null).build();
    }

    @Override
    protected ResourceNotFoundException notFoundException(String resourceId) {
        return ExceptionsHelper.missingTrainedModel(resourceId);
    }

    @Override
    protected void doExecute(Task task, GetTrainedModelsAction.Request request,
                             ActionListener<GetTrainedModelsAction.Response> listener) {
        searchResources(request, ActionListener.wrap(
            queryPage -> listener.onResponse(new GetTrainedModelsAction.Response(queryPage)),
            listener::onFailure
        ));
    }

    @Override
    protected String executionOrigin() {
        return ML_ORIGIN;
    }

    @Override
    protected String extractIdFromResource(TrainedModelConfig config) {
        return config.getModelId();
    }

    @Override
    protected SearchSourceBuilder customSearchOptions(SearchSourceBuilder searchSourceBuilder) {
        return searchSourceBuilder.sort("_index", SortOrder.DESC);
    }
}
