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
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.DecisionAction;
import org.elasticsearch.xpack.inference.action.task.StreamingTaskManager;
import org.elasticsearch.xpack.inference.registry.InferenceEndpointRegistry;

public class TransportDecisionAction extends BaseTransportInferenceAction<DecisionAction.Request> {

    @Inject
    public TransportDecisionAction(
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
            DecisionAction.NAME,
            transportService,
            actionFilters,
            licenseState,
            inferenceEndpointRegistry,
            serviceRegistry,
            inferenceStats,
            streamingTaskManager,
            DecisionAction.Request::new,
            threadPool
        );
    }

    @Override
    protected boolean isInvalidTaskTypeForInferenceEndpoint(DecisionAction.Request request, Model model) {
        assert request.getTaskType().isAnyOrSame(TaskType.DECISION);
        return model.getTaskType() != TaskType.DECISION;
    }

    @Override
    protected ElasticsearchStatusException createInvalidTaskTypeException(DecisionAction.Request request, Model model) {
        return new ElasticsearchStatusException(
            "Incompatible task_type for decision API, the inference endpoint [{}] has task type [{}], expected [{}]",
            RestStatus.BAD_REQUEST,
            request.getInferenceEntityId(),
            model.getTaskType(),
            TaskType.DECISION
        );
    }

    @Override
    protected void doInference(
        Model model,
        DecisionAction.Request request,
        InferenceService service,
        TaskId taskId,
        ActionListener<InferenceServiceResults> listener
    ) {
        service.decisionInfer(model, request.getDecisionRequest(), request.getTimeout(), taskId, listener);
    }
}
