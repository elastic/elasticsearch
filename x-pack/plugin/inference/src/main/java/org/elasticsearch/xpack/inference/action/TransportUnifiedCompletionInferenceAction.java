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
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.UnifiedCompletionAction;
import org.elasticsearch.xpack.inference.action.task.StreamingTaskManager;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.telemetry.InferenceStats;

public class TransportUnifiedCompletionInferenceAction extends BaseTransportInferenceAction<UnifiedCompletionAction.Request> {

    @Inject
    public TransportUnifiedCompletionInferenceAction(
        TransportService transportService,
        ActionFilters actionFilters,
        XPackLicenseState licenseState,
        ModelRegistry modelRegistry,
        InferenceServiceRegistry serviceRegistry,
        InferenceStats inferenceStats,
        StreamingTaskManager streamingTaskManager
    ) {
        super(
            UnifiedCompletionAction.NAME,
            transportService,
            actionFilters,
            licenseState,
            modelRegistry,
            serviceRegistry,
            inferenceStats,
            streamingTaskManager,
            UnifiedCompletionAction.Request::new
        );
    }

    @Override
    protected boolean isInvalidTaskTypeForInferenceEndpoint(UnifiedCompletionAction.Request request, UnparsedModel unparsedModel) {
        return request.getTaskType().isAnyOrSame(TaskType.CHAT_COMPLETION) == false || unparsedModel.taskType() != TaskType.CHAT_COMPLETION;
    }

    @Override
    protected ElasticsearchStatusException createInvalidTaskTypeException(
        UnifiedCompletionAction.Request request,
        UnparsedModel unparsedModel
    ) {
        return new ElasticsearchStatusException(
            "Incompatible task_type for unified API, the requested type [{}] must be one of [{}]",
            RestStatus.BAD_REQUEST,
            request.getTaskType(),
            TaskType.CHAT_COMPLETION.toString()
        );
    }

    @Override
    protected void doInference(
        Model model,
        UnifiedCompletionAction.Request request,
        InferenceService service,
        ActionListener<InferenceServiceResults> listener
    ) {
        service.unifiedCompletionInfer(model, request.getUnifiedCompletionRequest(), null, listener);
    }
}
