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
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.telemetry.InferenceStats;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.RerankAction;
import org.elasticsearch.xpack.inference.action.task.StreamingTaskManager;
import org.elasticsearch.xpack.inference.registry.InferenceEndpointRegistry;

import static org.elasticsearch.inference.InferenceString.textValue;
import static org.elasticsearch.inference.InferenceString.toStringList;

public class TransportRerankAction extends BaseTransportInferenceAction<RerankAction.Request> {

    @Inject
    public TransportRerankAction(
        TransportService transportService,
        ActionFilters actionFilters,
        XPackLicenseState licenseState,
        InferenceEndpointRegistry inferenceEndpointRegistry,
        InferenceServiceRegistry serviceRegistry,
        InferenceStats inferenceStats,
        StreamingTaskManager streamingTaskManager,
        ThreadPool threadPool
    ) {
        super(
            RerankAction.NAME,
            transportService,
            actionFilters,
            licenseState,
            inferenceEndpointRegistry,
            serviceRegistry,
            inferenceStats,
            streamingTaskManager,
            RerankAction.Request::new,
            threadPool
        );
    }

    @Override
    protected boolean isInvalidTaskTypeForInferenceEndpoint(RerankAction.Request request, Model model) {
        assert request.getTaskType().isAnyOrSame(TaskType.RERANK);
        return model.getTaskType() != TaskType.RERANK;
    }

    @Override
    protected ElasticsearchStatusException createInvalidTaskTypeException(RerankAction.Request request, Model model) {
        return new ElasticsearchStatusException(
            "Incompatible task_type for rerank API, the inference endpoint [{}] has task type [{}], expected [{}]",
            RestStatus.BAD_REQUEST,
            request.getInferenceEntityId(),
            model.getTaskType(),
            TaskType.RERANK
        );
    }

    @Override
    protected void doInference(
        Model model,
        RerankAction.Request request,
        InferenceService service,
        ActionListener<InferenceServiceResults> listener
    ) {
        var rerankRequest = request.getRerankRequest();
        if (service.supportsNewRerankCodePath()) {
            service.rerankInfer(model, rerankRequest, request.getTimeout(), listener);
        } else {
            service.infer(
                model,
                textValue(rerankRequest.query()),
                rerankRequest.returnDocuments(),
                rerankRequest.topN(),
                toStringList(rerankRequest.inputs()),
                request.isStreaming(),
                rerankRequest.taskSettings(),
                InputType.UNSPECIFIED,
                request.getTimeout(),
                listener
            );
        }
    }
}
