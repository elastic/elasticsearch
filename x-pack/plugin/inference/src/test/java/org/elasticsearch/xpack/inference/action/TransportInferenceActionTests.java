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
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.UnparsedModel;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.action.task.StreamingTaskManager;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.telemetry.InferenceStats;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Flow;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportInferenceActionTests extends ESTestCase {
    private static final String serviceId = "serviceId";
    private static final TaskType taskType = TaskType.COMPLETION;
    private static final String inferenceId = "inferenceEntityId";
    private ModelRegistry modelRegistry;
    private InferenceServiceRegistry serviceRegistry;
    private InferenceStats inferenceStats;
    private StreamingTaskManager streamingTaskManager;
    private TransportInferenceAction action;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        TransportService transportService = mock();
        ActionFilters actionFilters = mock();
        modelRegistry = mock();
        serviceRegistry = mock();
        inferenceStats = new InferenceStats(mock(), mock());
        streamingTaskManager = mock();
        action = new TransportInferenceAction(
            transportService,
            actionFilters,
            modelRegistry,
            serviceRegistry,
            inferenceStats,
            streamingTaskManager
        );
    }

    public void testMetricsAfterModelRegistryError() {
        var expectedException = new IllegalStateException("hello");
        var expectedError = expectedException.getClass().getSimpleName();

        doAnswer(ans -> {
            ActionListener<?> listener = ans.getArgument(1);
            listener.onFailure(expectedException);
            return null;
        }).when(modelRegistry).getModelWithSecrets(any(), any());

        var listener = doExecute(taskType);
        verify(listener).onFailure(same(expectedException));

        verify(inferenceStats.inferenceDuration()).record(anyLong(), assertArg(attributes -> {
            assertThat(attributes.get("service"), nullValue());
            assertThat(attributes.get("task_type"), nullValue());
            assertThat(attributes.get("model_id"), nullValue());
            assertThat(attributes.get("status_code"), nullValue());
            assertThat(attributes.get("error.type"), is(expectedError));
        }));
    }

    private ActionListener<InferenceAction.Response> doExecute(TaskType taskType) {
        return doExecute(taskType, false);
    }

    private ActionListener<InferenceAction.Response> doExecute(TaskType taskType, boolean stream) {
        InferenceAction.Request request = mock();
        when(request.getInferenceEntityId()).thenReturn(inferenceId);
        when(request.getTaskType()).thenReturn(taskType);
        when(request.isStreaming()).thenReturn(stream);
        ActionListener<InferenceAction.Response> listener = mock();
        action.doExecute(mock(), request, listener);
        return listener;
    }

    public void testMetricsAfterMissingService() {
        mockModelRegistry(taskType);

        when(serviceRegistry.getService(any())).thenReturn(Optional.empty());

        var listener = doExecute(taskType);

        verify(listener).onFailure(assertArg(e -> {
            assertThat(e, isA(ElasticsearchStatusException.class));
            assertThat(e.getMessage(), is("Unknown service [" + serviceId + "] for model [" + inferenceId + "]. "));
            assertThat(((ElasticsearchStatusException) e).status(), is(RestStatus.BAD_REQUEST));
        }));
        verify(inferenceStats.inferenceDuration()).record(anyLong(), assertArg(attributes -> {
            assertThat(attributes.get("service"), is(serviceId));
            assertThat(attributes.get("task_type"), is(taskType.toString()));
            assertThat(attributes.get("model_id"), nullValue());
            assertThat(attributes.get("status_code"), is(RestStatus.BAD_REQUEST.getStatus()));
            assertThat(attributes.get("error.type"), is(String.valueOf(RestStatus.BAD_REQUEST.getStatus())));
        }));
    }

    private void mockModelRegistry(TaskType expectedTaskType) {
        var unparsedModel = new UnparsedModel(inferenceId, expectedTaskType, serviceId, Map.of(), Map.of());
        doAnswer(ans -> {
            ActionListener<UnparsedModel> listener = ans.getArgument(1);
            listener.onResponse(unparsedModel);
            return null;
        }).when(modelRegistry).getModelWithSecrets(any(), any());
    }

    public void testMetricsAfterUnknownTaskType() {
        var modelTaskType = TaskType.RERANK;
        var requestTaskType = TaskType.SPARSE_EMBEDDING;
        mockModelRegistry(modelTaskType);
        when(serviceRegistry.getService(any())).thenReturn(Optional.of(mock()));

        var listener = doExecute(requestTaskType);

        verify(listener).onFailure(assertArg(e -> {
            assertThat(e, isA(ElasticsearchStatusException.class));
            assertThat(
                e.getMessage(),
                is(
                    "Incompatible task_type, the requested type ["
                        + requestTaskType
                        + "] does not match the model type ["
                        + modelTaskType
                        + "]"
                )
            );
            assertThat(((ElasticsearchStatusException) e).status(), is(RestStatus.BAD_REQUEST));
        }));
        verify(inferenceStats.inferenceDuration()).record(anyLong(), assertArg(attributes -> {
            assertThat(attributes.get("service"), is(serviceId));
            assertThat(attributes.get("task_type"), is(modelTaskType.toString()));
            assertThat(attributes.get("model_id"), nullValue());
            assertThat(attributes.get("status_code"), is(RestStatus.BAD_REQUEST.getStatus()));
            assertThat(attributes.get("error.type"), is(String.valueOf(RestStatus.BAD_REQUEST.getStatus())));
        }));
    }

    public void testMetricsAfterInferError() {
        var expectedException = new IllegalStateException("hello");
        var expectedError = expectedException.getClass().getSimpleName();
        mockService(listener -> listener.onFailure(expectedException));

        var listener = doExecute(taskType);

        verify(listener).onFailure(same(expectedException));
        verify(inferenceStats.inferenceDuration()).record(anyLong(), assertArg(attributes -> {
            assertThat(attributes.get("service"), is(serviceId));
            assertThat(attributes.get("task_type"), is(taskType.toString()));
            assertThat(attributes.get("model_id"), nullValue());
            assertThat(attributes.get("status_code"), nullValue());
            assertThat(attributes.get("error.type"), is(expectedError));
        }));
    }

    public void testMetricsAfterStreamUnsupported() {
        var expectedStatus = RestStatus.METHOD_NOT_ALLOWED;
        var expectedError = String.valueOf(expectedStatus.getStatus());
        mockService(l -> {});

        var listener = doExecute(taskType, true);

        verify(listener).onFailure(assertArg(e -> {
            assertThat(e, isA(ElasticsearchStatusException.class));
            var ese = (ElasticsearchStatusException) e;
            assertThat(ese.getMessage(), is("Streaming is not allowed for service [" + serviceId + "]."));
            assertThat(ese.status(), is(expectedStatus));
        }));
        verify(inferenceStats.inferenceDuration()).record(anyLong(), assertArg(attributes -> {
            assertThat(attributes.get("service"), is(serviceId));
            assertThat(attributes.get("task_type"), is(taskType.toString()));
            assertThat(attributes.get("model_id"), nullValue());
            assertThat(attributes.get("status_code"), is(expectedStatus.getStatus()));
            assertThat(attributes.get("error.type"), is(expectedError));
        }));
    }

    public void testMetricsAfterInferSuccess() {
        mockService(listener -> listener.onResponse(mock()));

        var listener = doExecute(taskType);

        verify(listener).onResponse(any());
        verify(inferenceStats.inferenceDuration()).record(anyLong(), assertArg(attributes -> {
            assertThat(attributes.get("service"), is(serviceId));
            assertThat(attributes.get("task_type"), is(taskType.toString()));
            assertThat(attributes.get("model_id"), nullValue());
            assertThat(attributes.get("status_code"), is(200));
            assertThat(attributes.get("error.type"), nullValue());
        }));
    }

    public void testMetricsAfterStreamInferSuccess() {
        mockStreamResponse(Flow.Subscriber::onComplete);
        verify(inferenceStats.inferenceDuration()).record(anyLong(), assertArg(attributes -> {
            assertThat(attributes.get("service"), is(serviceId));
            assertThat(attributes.get("task_type"), is(taskType.toString()));
            assertThat(attributes.get("model_id"), nullValue());
            assertThat(attributes.get("status_code"), is(200));
            assertThat(attributes.get("error.type"), nullValue());
        }));
    }

    public void testMetricsAfterStreamInferFailure() {
        var expectedException = new IllegalStateException("hello");
        var expectedError = expectedException.getClass().getSimpleName();
        mockStreamResponse(subscriber -> {
            subscriber.subscribe(mock());
            subscriber.onError(expectedException);
        });
        verify(inferenceStats.inferenceDuration()).record(anyLong(), assertArg(attributes -> {
            assertThat(attributes.get("service"), is(serviceId));
            assertThat(attributes.get("task_type"), is(taskType.toString()));
            assertThat(attributes.get("model_id"), nullValue());
            assertThat(attributes.get("status_code"), nullValue());
            assertThat(attributes.get("error.type"), is(expectedError));
        }));
    }

    public void testMetricsAfterStreamCancel() {
        var response = mockStreamResponse(s -> s.onSubscribe(mock()));
        response.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                subscription.cancel();
            }

            @Override
            public void onNext(ChunkedToXContent item) {

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onComplete() {

            }
        });

        verify(inferenceStats.inferenceDuration()).record(anyLong(), assertArg(attributes -> {
            assertThat(attributes.get("service"), is(serviceId));
            assertThat(attributes.get("task_type"), is(taskType.toString()));
            assertThat(attributes.get("model_id"), nullValue());
            assertThat(attributes.get("status_code"), is(200));
            assertThat(attributes.get("error.type"), nullValue());
        }));
    }

    private Flow.Publisher<ChunkedToXContent> mockStreamResponse(Consumer<Flow.Processor<?, ?>> action) {
        mockService(true, Set.of(), listener -> {
            Flow.Processor<ChunkedToXContent, ChunkedToXContent> taskProcessor = mock();
            doAnswer(innerAns -> {
                action.accept(innerAns.getArgument(0));
                return null;
            }).when(taskProcessor).subscribe(any());
            when(streamingTaskManager.<ChunkedToXContent>create(any(), any())).thenReturn(taskProcessor);
            var inferenceServiceResults = mock(InferenceServiceResults.class);
            when(inferenceServiceResults.publisher()).thenReturn(mock());
            listener.onResponse(inferenceServiceResults);
        });

        var listener = doExecute(taskType, true);
        var captor = ArgumentCaptor.forClass(InferenceAction.Response.class);
        verify(listener).onResponse(captor.capture());
        assertTrue(captor.getValue().isStreaming());
        assertNotNull(captor.getValue().publisher());
        return captor.getValue().publisher();
    }

    private void mockService(Consumer<ActionListener<InferenceServiceResults>> listenerAction) {
        mockService(false, Set.of(), listenerAction);
    }

    private void mockService(
        boolean stream,
        Set<TaskType> supportedStreamingTasks,
        Consumer<ActionListener<InferenceServiceResults>> listenerAction
    ) {
        InferenceService service = mock();
        Model model = mockModel();
        when(service.parsePersistedConfigWithSecrets(any(), any(), any(), any())).thenReturn(model);
        when(service.name()).thenReturn(serviceId);

        when(service.canStream(any())).thenReturn(stream);
        when(service.supportedStreamingTasks()).thenReturn(supportedStreamingTasks);
        doAnswer(ans -> {
            listenerAction.accept(ans.getArgument(7));
            return null;
        }).when(service).infer(any(), any(), any(), anyBoolean(), any(), any(), any(), any());
        mockModelAndServiceRegistry(service);
    }

    private Model mockModel() {
        Model model = mock();
        ModelConfigurations modelConfigurations = mock();
        when(modelConfigurations.getService()).thenReturn(serviceId);
        when(model.getConfigurations()).thenReturn(modelConfigurations);
        when(model.getTaskType()).thenReturn(taskType);
        when(model.getServiceSettings()).thenReturn(mock());
        return model;
    }

    private void mockModelAndServiceRegistry(InferenceService service) {
        var unparsedModel = new UnparsedModel(inferenceId, taskType, serviceId, Map.of(), Map.of());
        doAnswer(ans -> {
            ActionListener<UnparsedModel> listener = ans.getArgument(1);
            listener.onResponse(unparsedModel);
            return null;
        }).when(modelRegistry).getModelWithSecrets(any(), any());

        when(serviceRegistry.getService(any())).thenReturn(Optional.of(service));
    }
}
