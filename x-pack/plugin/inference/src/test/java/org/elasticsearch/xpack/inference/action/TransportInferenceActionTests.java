/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.telemetry.InferenceStats;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.action.task.StreamingTaskManager;
import org.elasticsearch.xpack.inference.registry.InferenceEndpointRegistry;

import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TransportInferenceActionTests extends BaseTransportInferenceActionTestCase<InferenceAction.Request> {

    public TransportInferenceActionTests() {
        super(TaskType.COMPLETION);
    }

    @Override
    protected BaseTransportInferenceAction<InferenceAction.Request> createAction(
        TransportService transportService,
        ActionFilters actionFilters,
        MockLicenseState licenseState,
        InferenceEndpointRegistry inferenceEndpointRegistry,
        InferenceServiceRegistry serviceRegistry,
        InferenceStats inferenceStats,
        StreamingTaskManager streamingTaskManager,
        ThreadPool threadPool
    ) {
        return new TransportInferenceAction(
            transportService,
            actionFilters,
            licenseState,
            inferenceEndpointRegistry,
            serviceRegistry,
            inferenceStats,
            streamingTaskManager,
            threadPool
        );
    }

    @Override
    protected InferenceAction.Request createRequest() {
        return mock(InferenceAction.Request.class);
    }

    public void testInferenceRunsAsChildOfActionTask() {
        mockService(false, Set.of(), listener -> listener.onResponse(mock()));
        var service = serviceRegistry.getService(serviceId).orElseThrow();

        doExecute(taskType);

        // doExecute runs the action with a mocked task, whose id is 0
        verify(service).infer(any(), any(), anyBoolean(), any(), any(), any(), eq(new TaskId("local_node", 0L)), any());
    }
}
