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
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.GetRerankerAction;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;

import java.util.concurrent.Executor;

public class TransportGetRerankerAction extends HandledTransportAction<GetRerankerAction.Request, GetRerankerAction.Response> {

    private final ModelRegistry modelRegistry;
    private final InferenceServiceRegistry serviceRegistry;
    private final Executor executor;

    @Inject
    public TransportGetRerankerAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        ModelRegistry modelRegistry,
        InferenceServiceRegistry serviceRegistry
    ) {
        super(GetRerankerAction.NAME, transportService, actionFilters, GetRerankerAction.Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.modelRegistry = modelRegistry;
        this.serviceRegistry = serviceRegistry;
        this.executor = threadPool.executor(InferencePlugin.UTILITY_THREAD_POOL_NAME);
    }

    @Override
    protected void doExecute(Task task, GetRerankerAction.Request request, ActionListener<GetRerankerAction.Response> listener) {

        SubscribableListener.<UnparsedModel>newForked(l -> modelRegistry.getModel(request.getInferenceEntityId(), l))
            .andThen((l2, model) -> {
                if (model.taskType() != TaskType.RERANK) {
                    l2.onFailure(
                        new ElasticsearchStatusException(
                            "Inference endpoint [{}] is not a reranker",
                            RestStatus.BAD_REQUEST,
                            request.getInferenceEntityId()
                        )
                    );
                    return;
                }

                var service = serviceRegistry.getService(model.service());
                l2.onResponse(new GetRerankerAction.Response(rerankWindowSize(service.get())));
            });
    }

    public int rerankWindowSize(InferenceService service) {
        return 0;
    }

}
