/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.InferenceServiceRegistry;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.telemetry.InferenceStats;
import org.elasticsearch.inference.telemetry.InferenceStatsTests;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.InferenceContext;
import org.elasticsearch.xpack.core.inference.action.BaseInferenceActionRequest;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.action.task.StreamingTaskManager;
import org.elasticsearch.xpack.inference.registry.InferenceEndpointRegistry;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public abstract class BaseTransportInferenceActionTestCase<Request extends BaseInferenceActionRequest> extends ESTestCase {
    private MockLicenseState licenseState;
    private InferenceEndpointRegistry inferenceEndpointRegistry;
    private StreamingTaskManager streamingTaskManager;
    private BaseTransportInferenceAction<Request> action;
    private ThreadPool threadPool;

    protected static final String serviceId = "serviceId";
    protected final TaskType taskType;
    protected static final String inferenceId = "inferenceEntityId";
    protected InferenceServiceRegistry serviceRegistry;
    protected InferenceStats inferenceStats;
    protected TransportService transportService;
    protected NodeClient nodeClient;

    public BaseTransportInferenceActionTestCase(TaskType taskType) {
        this.taskType = taskType;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        ActionFilters actionFilters = mock();
        threadPool = mock();
        nodeClient = mock();
        transportService = mock();
        licenseState = mock();
        inferenceEndpointRegistry = mock();
        serviceRegistry = mock();
        inferenceStats = InferenceStatsTests.mockInferenceStats();
        streamingTaskManager = mock();

        action = createAction(
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

        mockValidLicenseState();
    }

    protected abstract BaseTransportInferenceAction<Request> createAction(
        TransportService transportService,
        ActionFilters actionFilters,
        MockLicenseState licenseState,
        InferenceEndpointRegistry inferenceEndpointRegistry,
        InferenceServiceRegistry serviceRegistry,
        InferenceStats inferenceStats,
        StreamingTaskManager streamingTaskManager,
        NodeClient nodeClient,
        ThreadPool threadPool
    );

    protected abstract Request createRequest();

    public void testMetricsAfterModelRegistryError() {
        var expectedException = new IllegalStateException("hello");
        var expectedError = expectedException.getClass().getSimpleName();

        doAnswer(ans -> {
            ActionListener<?> listener = ans.getArgument(1);
            listener.onFailure(expectedException);
            return null;
        }).when(inferenceEndpointRegistry).getEndpoint(any(), any());

        doExecute(taskType);

        verify(inferenceStats.inferenceDuration()).record(anyLong(), assertArg(attributes -> {
            assertThat(attributes.get("service"), nullValue());
            assertThat(attributes.get("task_type"), nullValue());
            assertThat(attributes.get("model_id"), nullValue());
            assertThat(attributes.get("status_code"), nullValue());
            assertThat(attributes.get("error.type"), is(expectedError));
            assertThat(attributes.get("rerouted"), nullValue());
            assertThat(attributes.get("node_id"), nullValue());
        }));
    }

    protected ActionListener<InferenceAction.Response> doExecute(TaskType taskType) {
        return doExecute(taskType, false);
    }

    protected ActionListener<InferenceAction.Response> doExecute(TaskType taskType, boolean stream) {
        Request request = createRequest();
        when(request.getInferenceEntityId()).thenReturn(inferenceId);
        when(request.getTaskType()).thenReturn(taskType);
        when(request.isStreaming()).thenReturn(stream);
        ActionListener<InferenceAction.Response> listener = spy(new ActionListener<>() {
            @Override
            public void onResponse(InferenceAction.Response o) {}

            @Override
            public void onFailure(Exception e) {}
        });
        action.doExecute(mock(), request, listener);
        return listener;
    }

    public void testMetricsAfterMissingService() {
        mockInferenceEndpointRegistry(taskType);

        when(serviceRegistry.getService(any())).thenReturn(Optional.empty());

        var listener = doExecute(taskType);

        verify(listener).onFailure(assertArg(e -> {
            assertThat(e, isA(ElasticsearchException.class));
            assertThat(e.getMessage(), is("Unknown service [" + serviceId + "] for model [" + inferenceId + "]"));
            assertThat(((ElasticsearchException) e).status(), is(RestStatus.BAD_REQUEST));
        }));
        verify(inferenceStats.inferenceDuration()).record(anyLong(), assertArg(attributes -> {
            assertThat(attributes.get("service"), is(serviceId));
            assertThat(attributes.get("task_type"), is(taskType.toString()));
            assertThat(attributes.get("model_id"), nullValue());
            assertThat(attributes.get("status_code"), is(RestStatus.BAD_REQUEST.getStatus()));
            assertThat(attributes.get("error.type"), is(String.valueOf(RestStatus.BAD_REQUEST.getStatus())));
            assertThat(attributes.get("rerouted"), nullValue());
            assertThat(attributes.get("node_id"), nullValue());
        }));
    }

    protected void mockInferenceEndpointRegistry(TaskType expectedTaskType) {
        doAnswer(ans -> {
            ActionListener<Model> listener = ans.getArgument(1);
            listener.onResponse(mockModel(expectedTaskType));
            return null;
        }).when(inferenceEndpointRegistry).getEndpoint(any(), any());
    }

    public void testMetricsAfterUnknownTaskType() {
        var modelTaskType = TaskType.RERANK;
        var requestTaskType = TaskType.SPARSE_EMBEDDING;
        mockInferenceEndpointRegistry(modelTaskType);
        when(serviceRegistry.getService(any())).thenReturn(Optional.of(mock()));

        var listener = doExecute(requestTaskType);

        verify(listener).onFailure(assertArg(e -> {
            assertThat(e, isA(ElasticsearchException.class));
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
            assertThat(((ElasticsearchException) e).status(), is(RestStatus.BAD_REQUEST));
        }));
        verify(inferenceStats.inferenceDuration()).record(anyLong(), assertArg(attributes -> {
            assertThat(attributes.get("service"), is(serviceId));
            assertThat(attributes.get("task_type"), is(modelTaskType.toString()));
            assertThat(attributes.get("model_id"), nullValue());
            assertThat(attributes.get("status_code"), is(RestStatus.BAD_REQUEST.getStatus()));
            assertThat(attributes.get("error.type"), is(String.valueOf(RestStatus.BAD_REQUEST.getStatus())));
            assertThat(attributes.get("rerouted"), nullValue());
            assertThat(attributes.get("node_id"), nullValue());
        }));
    }

    public void testMetricsAfterInferError() {
        var expectedException = new IllegalStateException("hello");
        var expectedError = expectedException.getClass().getSimpleName();
        mockService(listener -> listener.onFailure(expectedException));

        var listener = doExecute(taskType);

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
            assertThat(e, isA(ElasticsearchException.class));
            var ese = (ElasticsearchException) e;
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
        mockStreamResponse(Flow.Subscriber::onComplete).subscribe(mock());
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
        mockStreamResponse(subscriber -> subscriber.onError(expectedException)).subscribe(mock());
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
            public void onNext(InferenceServiceResults.Result item) {

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

    public void testProductUseCaseHeaderPresentInThreadContextIfPresent() {
        String productUseCase = "product-use-case";

        // We need to use real instances instead of mocks as these are final classes
        InferenceContext context = new InferenceContext(productUseCase);
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        when(threadPool.getThreadContext()).thenReturn(threadContext);

        mockInferenceEndpointRegistry(taskType);
        mockService(listener -> listener.onResponse(mock()));

        Request request = createRequest();
        when(request.getContext()).thenReturn(context);
        when(request.getInferenceEntityId()).thenReturn(inferenceId);
        when(request.getTaskType()).thenReturn(taskType);
        when(request.isStreaming()).thenReturn(false);

        ActionListener<InferenceAction.Response> listener = spy(new ActionListener<>() {
            @Override
            public void onResponse(InferenceAction.Response o) {}

            @Override
            public void onFailure(Exception e) {}
        });

        action.doExecute(mock(), request, listener);

        // Verify the product use case header was set in the thread context
        assertThat(threadContext.getHeader(InferencePlugin.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER), is(productUseCase));
    }

    protected Flow.Publisher<InferenceServiceResults.Result> mockStreamResponse(Consumer<Flow.Subscriber<?>> action) {
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

    protected void mockService(Consumer<ActionListener<InferenceServiceResults>> listenerAction) {
        mockService(false, Set.of(), listenerAction);
    }

    protected void mockService(
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
            listenerAction.accept(ans.getArgument(9));
            return null;
        }).when(service).infer(any(), any(), anyBoolean(), any(), any(), anyBoolean(), any(), any(), any(), any());
        doAnswer(ans -> {
            listenerAction.accept(ans.getArgument(3));
            return null;
        }).when(service).unifiedCompletionInfer(any(), any(), any(), any());
        doAnswer(ans -> {
            listenerAction.accept(ans.getArgument(3));
            return null;
        }).when(service).embeddingInfer(any(), any(), any(), any());
        mockInferenceEndpointRegistry(taskType);
        when(serviceRegistry.getService(any())).thenReturn(Optional.of(service));
    }

    protected Model mockModel() {
        return mockModel(taskType);
    }

    protected Model mockModel(TaskType expectedTaskType) {
        Model model = mock();
        ModelConfigurations modelConfigurations = mock();
        when(modelConfigurations.getService()).thenReturn(serviceId);
        when(model.getConfigurations()).thenReturn(modelConfigurations);
        when(model.getTaskType()).thenReturn(expectedTaskType);
        when(model.getServiceSettings()).thenReturn(mock());
        when(model.getInferenceEntityId()).thenReturn(inferenceId);
        return model;
    }

    protected void mockValidLicenseState() {
        when(licenseState.isAllowed(InferencePlugin.INFERENCE_API_FEATURE)).thenReturn(true);
    }
}
