/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.EmbeddingRequest;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.telemetry.InferenceStats;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.GenericDenseEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.action.task.StreamingTaskManager;
import org.elasticsearch.xpack.inference.registry.InferenceEndpointRegistry;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class TransportInferenceActionTests extends BaseTransportInferenceActionTestCase<InferenceAction.Request> {

    private TransportInferenceAction action;

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
        action = new TransportInferenceAction(
            transportService,
            actionFilters,
            licenseState,
            inferenceEndpointRegistry,
            serviceRegistry,
            inferenceStats,
            streamingTaskManager,
            threadPool
        );
        return action;
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

    public void testEmbeddingEndpointWithIngestInput() {
        assertEmbeddingInference(InputType.INTERNAL_INGEST);
    }

    public void testEmbeddingEndpointWithSearchInput() {
        assertEmbeddingInference(InputType.INTERNAL_SEARCH);
    }

    private void assertEmbeddingInference(InputType inputType) {
        var results = new GenericDenseEmbeddingFloatResults(
            List.of(
                new GenericDenseEmbeddingFloatResults.Embedding(new float[] { 0.25f, 0.75f }),
                new GenericDenseEmbeddingFloatResults.Embedding(new float[] { 0.5f, 1.0f })
            )
        );
        mockService(listener -> listener.onResponse(results));
        mockInferenceEndpointRegistry(TaskType.EMBEDDING);
        var service = serviceRegistry.getService(serviceId).orElseThrow();
        var timeout = TimeValue.timeValueSeconds(42);
        var taskSettings = Map.<String, Object>of("user", "test-user");
        var request = new InferenceAction.Request(
            TaskType.ANY,
            inferenceId,
            List.of("first input", "second input"),
            taskSettings,
            inputType,
            timeout,
            false
        );
        var future = new PlainActionFuture<InferenceAction.Response>();

        action.doExecute(mock(), request, future);

        assertTrue(future.isDone());
        assertSame(results, future.actionGet().getResults());
        verify(service).embeddingInfer(
            any(),
            eq(
                new EmbeddingRequest(
                    List.of(new InferenceStringGroup("first input"), new InferenceStringGroup("second input")),
                    inputType,
                    taskSettings
                )
            ),
            eq(timeout),
            eq(new TaskId("local_node", 0L)),
            any()
        );
        verify(service, never()).infer(any(), any(), anyBoolean(), any(), any(), any(), any(), any());
    }

    public void testEmbeddingEndpointFailureIsPropagated() {
        var failure = new IllegalStateException("inference failed");
        mockService(listener -> listener.onFailure(failure));
        mockInferenceEndpointRegistry(TaskType.EMBEDDING);
        var request = new InferenceAction.Request(
            TaskType.ANY,
            inferenceId,
            List.of("input"),
            Map.of(),
            InputType.INTERNAL_INGEST,
            TimeValue.timeValueSeconds(30),
            false
        );
        var future = new PlainActionFuture<InferenceAction.Response>();

        action.doExecute(mock(), request, future);

        assertTrue(future.isDone());
        assertSame(failure, expectThrows(IllegalStateException.class, future::actionGet));
        verify(serviceRegistry.getService(serviceId).orElseThrow()).embeddingInfer(any(), any(), any(), any(), any());
    }

    public void testTextEmbeddingEndpointUsesLegacyInference() {
        mockService(listener -> listener.onResponse(mock()));
        mockInferenceEndpointRegistry(TaskType.TEXT_EMBEDDING);

        var listener = doExecute(TaskType.ANY);

        verify(listener).onResponse(any());
        var service = serviceRegistry.getService(serviceId).orElseThrow();
        verify(service).infer(any(), any(), anyBoolean(), any(), any(), any(), any(), any());
        verify(service, never()).embeddingInfer(any(), any(), any(), any(), any());
    }
}
