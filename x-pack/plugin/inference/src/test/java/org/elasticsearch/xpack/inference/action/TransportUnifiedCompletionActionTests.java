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
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.UnifiedCompletionAction;
import org.elasticsearch.xpack.core.inference.results.UnifiedChatCompletionException;
import org.elasticsearch.xpack.inference.action.task.StreamingTaskManager;
import org.elasticsearch.xpack.inference.common.InferenceServiceRateLimitCalculator;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.telemetry.InferenceStats;

import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportUnifiedCompletionActionTests extends BaseTransportInferenceActionTestCase<UnifiedCompletionAction.Request> {

    public TransportUnifiedCompletionActionTests() {
        super(TaskType.CHAT_COMPLETION);
    }

    @Override
    protected BaseTransportInferenceAction<UnifiedCompletionAction.Request> createAction(
        TransportService transportService,
        ActionFilters actionFilters,
        MockLicenseState licenseState,
        ModelRegistry modelRegistry,
        InferenceServiceRegistry serviceRegistry,
        InferenceStats inferenceStats,
        StreamingTaskManager streamingTaskManager,
        InferenceServiceRateLimitCalculator inferenceServiceRateLimitCalculator,
        NodeClient nodeClient,
        ThreadPool threadPool
    ) {
        return new TransportUnifiedCompletionInferenceAction(
            transportService,
            actionFilters,
            licenseState,
            modelRegistry,
            serviceRegistry,
            inferenceStats,
            streamingTaskManager,
            inferenceServiceRateLimitCalculator,
            nodeClient,
            threadPool
        );
    }

    @Override
    protected UnifiedCompletionAction.Request createRequest() {
        return mock(UnifiedCompletionAction.Request.class);
    }

    public void testThrows_IncompatibleTaskTypeException_WhenUsingATextEmbeddingInferenceEndpoint() {
        var modelTaskType = TaskType.TEXT_EMBEDDING;
        var requestTaskType = TaskType.TEXT_EMBEDDING;
        mockModelRegistry(modelTaskType);
        when(serviceRegistry.getService(any())).thenReturn(Optional.of(mock()));

        var listener = doExecute(requestTaskType);

        verify(listener).onFailure(assertArg(e -> {
            assertThat(e, isA(UnifiedChatCompletionException.class));
            assertThat(
                e.getMessage(),
                is("Incompatible task_type for unified API, the requested type [" + requestTaskType + "] must be one of [chat_completion]")
            );
            assertThat(((UnifiedChatCompletionException) e).status(), is(RestStatus.BAD_REQUEST));
        }));
        verify(inferenceStats.inferenceDuration()).record(anyLong(), assertArg(attributes -> {
            assertThat(attributes.get("service"), is(serviceId));
            assertThat(attributes.get("task_type"), is(modelTaskType.toString()));
            assertThat(attributes.get("model_id"), nullValue());
            assertThat(attributes.get("status_code"), is(RestStatus.BAD_REQUEST.getStatus()));
            assertThat(attributes.get("error.type"), is(String.valueOf(RestStatus.BAD_REQUEST.getStatus())));
        }));
    }

    public void testThrows_IncompatibleTaskTypeException_WhenUsingRequestIsAny_ModelIsTextEmbedding() {
        var modelTaskType = TaskType.ANY;
        var requestTaskType = TaskType.TEXT_EMBEDDING;
        mockModelRegistry(modelTaskType);
        when(serviceRegistry.getService(any())).thenReturn(Optional.of(mock()));

        var listener = doExecute(requestTaskType);

        verify(listener).onFailure(assertArg(e -> {
            assertThat(e, isA(UnifiedChatCompletionException.class));
            assertThat(
                e.getMessage(),
                is("Incompatible task_type for unified API, the requested type [" + requestTaskType + "] must be one of [chat_completion]")
            );
            assertThat(((UnifiedChatCompletionException) e).status(), is(RestStatus.BAD_REQUEST));
        }));
        verify(inferenceStats.inferenceDuration()).record(anyLong(), assertArg(attributes -> {
            assertThat(attributes.get("service"), is(serviceId));
            assertThat(attributes.get("task_type"), is(modelTaskType.toString()));
            assertThat(attributes.get("model_id"), nullValue());
            assertThat(attributes.get("status_code"), is(RestStatus.BAD_REQUEST.getStatus()));
            assertThat(attributes.get("error.type"), is(String.valueOf(RestStatus.BAD_REQUEST.getStatus())));
        }));
    }

    public void testMetricsAfterUnifiedInferSuccess_WithRequestTaskTypeAny() {
        mockModelRegistry(TaskType.COMPLETION);
        mockService(listener -> listener.onResponse(mock()));

        var listener = doExecute(TaskType.ANY);

        verify(listener).onResponse(any());
        verify(inferenceStats.inferenceDuration()).record(anyLong(), assertArg(attributes -> {
            assertThat(attributes.get("service"), is(serviceId));
            assertThat(attributes.get("task_type"), is(taskType.toString()));
            assertThat(attributes.get("model_id"), nullValue());
            assertThat(attributes.get("status_code"), is(200));
            assertThat(attributes.get("error.type"), nullValue());
        }));
    }
}
