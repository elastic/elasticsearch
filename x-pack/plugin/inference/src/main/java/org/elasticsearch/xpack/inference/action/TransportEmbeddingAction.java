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
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.telemetry.InferenceStats;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.EmbeddingAction;
import org.elasticsearch.xpack.inference.action.task.StreamingTaskManager;
import org.elasticsearch.xpack.inference.registry.InferenceEndpointRegistry;

public class TransportEmbeddingAction extends BaseTransportInferenceAction<EmbeddingAction.Request> {

    @Inject
    public TransportEmbeddingAction(
        TransportService transportService,
        ActionFilters actionFilters,
        XPackLicenseState licenseState,
        InferenceEndpointRegistry inferenceEndpointRegistry,
        InferenceServiceRegistry serviceRegistry,
        InferenceStats inferenceStats,
        StreamingTaskManager streamingTaskManager,
        NodeClient nodeClient,
        ThreadPool threadPool
    ) {
        super(
            EmbeddingAction.NAME,
            transportService,
            actionFilters,
            licenseState,
            inferenceEndpointRegistry,
            serviceRegistry,
            inferenceStats,
            streamingTaskManager,
            EmbeddingAction.Request::new,
            nodeClient,
            threadPool
        );
    }

    @Override
    protected boolean isInvalidTaskTypeForInferenceEndpoint(EmbeddingAction.Request request, Model model) {
        return request.getTaskType().isAnyOrSame(TaskType.EMBEDDING) == false || model.getTaskType() != TaskType.EMBEDDING;
    }

    @Override
    protected ElasticsearchStatusException createInvalidTaskTypeException(EmbeddingAction.Request request, Model model) {
        return new ElasticsearchStatusException(
            "Incompatible task_type for embedding API, the requested type [{}] must be one of [{}]",
            RestStatus.BAD_REQUEST,
            request.getTaskType(),
            TaskType.EMBEDDING.toString()
        );
    }

    @Override
    protected void doInference(
        Model model,
        EmbeddingAction.Request request,
        InferenceService service,
        ActionListener<InferenceServiceResults> listener
    ) {
        service.embeddingInfer(model, request.getEmbeddingRequest(), request.getTimeout(), listener);
    }
}
