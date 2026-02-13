/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.telemetry.InferenceStats;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.action.task.StreamingTaskManager;
import org.elasticsearch.xpack.inference.registry.InferenceEndpointRegistry;

import static org.mockito.Mockito.mock;

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
        NodeClient nodeClient,
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
            nodeClient,
            threadPool
        );
    }

    @Override
    protected InferenceAction.Request createRequest() {
        return mock(InferenceAction.Request.class);
    }
}
