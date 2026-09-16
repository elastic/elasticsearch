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
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.telemetry.InferenceStats;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.DocumentExtractionAction;
import org.elasticsearch.xpack.inference.action.task.StreamingTaskManager;
import org.elasticsearch.xpack.inference.registry.InferenceEndpointRegistry;

public class TransportDocumentExtractionAction extends BaseTransportInferenceAction<DocumentExtractionAction.Request> {

    @Inject
    public TransportDocumentExtractionAction(
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
            DocumentExtractionAction.NAME,
            transportService,
            actionFilters,
            licenseState,
            inferenceEndpointRegistry,
            serviceRegistry,
            inferenceStats,
            streamingTaskManager,
            DocumentExtractionAction.Request::new,
            threadPool
        );
    }

    @Override
    protected boolean isInvalidTaskTypeForInferenceEndpoint(DocumentExtractionAction.Request request, Model model) {
        assert request.getTaskType().isAnyOrSame(TaskType.DOCUMENT_EXTRACTION);
        return model.getTaskType() != TaskType.DOCUMENT_EXTRACTION;
    }

    @Override
    protected ElasticsearchStatusException createInvalidTaskTypeException(DocumentExtractionAction.Request request, Model model) {
        return new ElasticsearchStatusException(
            "Incompatible task_type for document extraction API, the inference endpoint [{}] has task type [{}], expected [{}]",
            RestStatus.BAD_REQUEST,
            request.getInferenceEntityId(),
            model.getTaskType(),
            TaskType.DOCUMENT_EXTRACTION
        );
    }

    @Override
    protected void doInference(
        Model model,
        DocumentExtractionAction.Request request,
        InferenceService service,
        ActionListener<InferenceServiceResults> listener
    ) {
        service.documentExtractionInfer(model, request.getDocumentExtractionRequest(), request.getTimeout(), listener);
    }
}
