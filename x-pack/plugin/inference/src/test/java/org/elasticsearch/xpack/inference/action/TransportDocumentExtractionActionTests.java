/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.telemetry.InferenceStats;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.DocumentExtractionAction;
import org.elasticsearch.xpack.inference.action.task.StreamingTaskManager;
import org.elasticsearch.xpack.inference.registry.InferenceEndpointRegistry;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportDocumentExtractionActionTests extends BaseTransportInferenceActionTestCase<DocumentExtractionAction.Request> {

    private static final Set<TaskType> NON_DOCUMENT_EXTRACTION_MODEL_TASK_TYPES = Arrays.stream(TaskType.values())
        .filter(t -> t.isAnyOrSame(TaskType.DOCUMENT_EXTRACTION) == false)
        .collect(Collectors.toSet());

    public TransportDocumentExtractionActionTests() {
        super(TaskType.DOCUMENT_EXTRACTION);
    }

    @Override
    protected BaseTransportInferenceAction<DocumentExtractionAction.Request> createAction(
        TransportService transportService,
        ActionFilters actionFilters,
        MockLicenseState licenseState,
        InferenceEndpointRegistry inferenceEndpointRegistry,
        InferenceServiceRegistry serviceRegistry,
        InferenceStats inferenceStats,
        StreamingTaskManager streamingTaskManager,
        ThreadPool threadPool
    ) {
        return new TransportDocumentExtractionAction(
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
    protected DocumentExtractionAction.Request createRequest() {
        return mock(DocumentExtractionAction.Request.class);
    }

    public void testThrowsIncompatibleTaskTypeException_whenUsingNonDocumentExtractionInferenceEndpoint() {
        var modelTaskType = randomFrom(NON_DOCUMENT_EXTRACTION_MODEL_TASK_TYPES);
        var requestTaskType = TaskType.ANY;  // Use ANY to satisfy model task type equality checks
        mockInferenceEndpointRegistry(modelTaskType);
        when(serviceRegistry.getService(any())).thenReturn(Optional.of(mock()));

        var listener = doExecute(requestTaskType);

        verify(listener).onFailure(assertArg(e -> {
            assertThat(e, isA(ElasticsearchStatusException.class));
            assertThat(
                e.getMessage(),
                is(
                    "Incompatible task_type for document extraction API, the inference endpoint ["
                        + inferenceId
                        + "] has task type ["
                        + modelTaskType.toString()
                        + "], expected [document_extraction]"
                )
            );
            assertThat(((ElasticsearchStatusException) e).status(), is(RestStatus.BAD_REQUEST));
        }));
        verify(mockInferenceDurationHistogram).record(anyLong(), assertArg(attributes -> {
            assertThat(attributes.get("service"), is(serviceId));
            assertThat(attributes.get("task_type"), is(modelTaskType.toString()));
            assertThat(attributes.get("model_id"), nullValue());
            assertThat(attributes.get("status_code"), is(RestStatus.BAD_REQUEST.getStatus()));
            assertThat(attributes.get("error_type"), is(String.valueOf(RestStatus.BAD_REQUEST.getStatus())));
        }));
    }

    public void testThrowsIncompatibleTaskTypeException_whenRequestTaskTypeIsUnsupported() {
        var modelTaskType = TaskType.DOCUMENT_EXTRACTION;
        var requestTaskType = randomFrom(NON_DOCUMENT_EXTRACTION_MODEL_TASK_TYPES);
        mockInferenceEndpointRegistry(modelTaskType);
        when(serviceRegistry.getService(any())).thenReturn(Optional.of(mock()));

        var listener = doExecute(requestTaskType);

        verify(listener).onFailure(assertArg(e -> {
            assertThat(e, isA(ElasticsearchStatusException.class));
            assertThat(
                e.getMessage(),
                is(
                    "Incompatible task_type, the requested type ["
                        + requestTaskType
                        + "] does not match the model type [document_extraction]"
                )
            );
            assertThat(((ElasticsearchStatusException) e).status(), is(RestStatus.BAD_REQUEST));
        }));
        verify(mockInferenceDurationHistogram).record(anyLong(), assertArg(attributes -> {
            assertThat(attributes.get("service"), is(serviceId));
            assertThat(attributes.get("task_type"), is(modelTaskType.toString()));
            assertThat(attributes.get("model_id"), nullValue());
            assertThat(attributes.get("status_code"), is(RestStatus.BAD_REQUEST.getStatus()));
            assertThat(attributes.get("error_type"), is(String.valueOf(RestStatus.BAD_REQUEST.getStatus())));
        }));
    }

    public void testMetricsAfterDocumentExtractionSuccess_withRequestTaskTypeAny() {
        mockInferenceEndpointRegistry(TaskType.DOCUMENT_EXTRACTION);
        mockService(listener -> listener.onResponse(mock()));

        var listener = doExecute(TaskType.ANY);

        verify(listener).onResponse(any());
        verify(mockInferenceDurationHistogram).record(anyLong(), assertArg(attributes -> {
            assertThat(attributes.get("service"), is(serviceId));
            assertThat(attributes.get("task_type"), is(taskType.toString()));
            assertThat(attributes.get("model_id"), nullValue());
            assertThat(attributes.get("status_code"), is(200));
            assertThat(attributes.get("error_type"), nullValue());
        }));
    }
}
