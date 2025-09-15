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
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.RerankingInferenceService;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.GetRerankerWindowSizeAction;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

public class TransportGetRerankerWindowSizeAction extends HandledTransportAction<
    GetRerankerWindowSizeAction.Request,
    GetRerankerWindowSizeAction.Response> {

    private final ModelRegistry modelRegistry;
    private final InferenceServiceRegistry serviceRegistry;

    @Inject
    public TransportGetRerankerWindowSizeAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        ModelRegistry modelRegistry,
        InferenceServiceRegistry serviceRegistry
    ) {
        super(
            GetRerankerWindowSizeAction.NAME,
            transportService,
            actionFilters,
            GetRerankerWindowSizeAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.modelRegistry = modelRegistry;
        this.serviceRegistry = serviceRegistry;
    }

    @Override
    protected void doExecute(
        Task task,
        GetRerankerWindowSizeAction.Request request,
        ActionListener<GetRerankerWindowSizeAction.Response> listener
    ) {

        SubscribableListener.<UnparsedModel>newForked(l -> modelRegistry.getModel(request.getInferenceEntityId(), l)).<
            GetRerankerWindowSizeAction.Response>andThen((l, unparsedModel) -> {
                if (unparsedModel.taskType() != TaskType.RERANK) {
                    throw new ElasticsearchStatusException(
                        "Inference endpoint [{}] does not have the {} task type",
                        RestStatus.BAD_REQUEST,
                        request.getInferenceEntityId(),
                        TaskType.RERANK
                    );
                }

                var service = serviceRegistry.getService(unparsedModel.service());
                if (service.isEmpty()) {
                    throw new ElasticsearchStatusException(
                        "Unknown service [{}] for inference endpoint [{}]",
                        RestStatus.BAD_REQUEST,
                        unparsedModel.service(),
                        request.getInferenceEntityId()
                    );
                }

                if (service.get() instanceof RerankingInferenceService rerankingInferenceService) {
                    var model = service.get()
                        .parsePersistedConfig(unparsedModel.inferenceEntityId(), unparsedModel.taskType(), unparsedModel.settings());

                    l.onResponse(
                        new GetRerankerWindowSizeAction.Response(
                            rerankWindowSize(rerankingInferenceService, model.getServiceSettings().modelId())
                        )
                    );
                } else {
                    throw new IllegalStateException(
                        "Inference endpoint ["
                            + request.getInferenceEntityId()
                            + "] has task type ["
                            + TaskType.RERANK
                            + "] but the service ["
                            + service.get().name()
                            + "] does not support reranking"
                    );
                }
            }).addListener(listener);
    }

    private int rerankWindowSize(RerankingInferenceService service, String modelId) {
        return service.rerankerWindowSize(modelId);
    }
}
