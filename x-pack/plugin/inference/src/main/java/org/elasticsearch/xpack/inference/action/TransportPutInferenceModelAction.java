/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.inference.Model;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.registry.ServiceRegistry;
import org.elasticsearch.xpack.inference.services.InferenceService;

import java.io.IOException;
import java.util.Map;

public class TransportPutInferenceModelAction extends TransportMasterNodeAction<
    PutInferenceModelAction.Request,
    PutInferenceModelAction.Response> {

    private final ModelRegistry modelRegistry;
    private final ServiceRegistry serviceRegistry;

    @Inject
    public TransportPutInferenceModelAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ModelRegistry modelRegistry,
        ServiceRegistry serviceRegistry
    ) {
        super(
            PutInferenceModelAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutInferenceModelAction.Request::new,
            indexNameExpressionResolver,
            PutInferenceModelAction.Response::new,
            ThreadPool.Names.SAME
        );
        this.modelRegistry = modelRegistry;
        this.serviceRegistry = serviceRegistry;
    }

    @Override
    protected void masterOperation(
        Task task,
        PutInferenceModelAction.Request request,
        ClusterState state,
        ActionListener<PutInferenceModelAction.Response> listener
    ) throws Exception {

        var requestAsMap = requestToMap(request);
        String serviceName = (String) requestAsMap.remove(Model.SERVICE);
        if (serviceName == null) {
            listener.onFailure(new ElasticsearchStatusException("Model configuration is missing a service", RestStatus.BAD_REQUEST));
            return;
        }

        var service = serviceRegistry.getService(serviceName);
        if (service.isEmpty()) {
            listener.onFailure(new ElasticsearchStatusException("Unknown service [{}]", RestStatus.BAD_REQUEST, serviceName));
            return;
        }

        var model = service.get().parseConfigStrict(request.getModelId(), request.getTaskType(), requestAsMap);
        // model is valid good to persist then start
        this.modelRegistry.storeModel(
            model,
            ActionListener.wrap(r -> { startModel(service.get(), model, listener); }, listener::onFailure)
        );
    }

    private static void startModel(InferenceService service, Model model, ActionListener<PutInferenceModelAction.Response> listener) {
        service.start(
            model,
            ActionListener.wrap(ok -> listener.onResponse(new PutInferenceModelAction.Response(model)), listener::onFailure)
        );
    }

    private Map<String, Object> requestToMap(PutInferenceModelAction.Request request) throws IOException {
        try (
            XContentParser parser = XContentHelper.createParser(
                XContentParserConfiguration.EMPTY,
                request.getContent(),
                request.getContentType()
            )
        ) {
            return parser.map();
        }
    }

    @Override
    protected ClusterBlockException checkBlock(PutInferenceModelAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
