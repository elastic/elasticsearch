/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkInferenceInput;
import org.elasticsearch.inference.ChunkedInference;
import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceInputs;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SenderServiceTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);

    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        threadPool = createThreadPool(inferenceUtilityPool());
    }

    @After
    public void shutdown() throws IOException {
        terminate(threadPool);
    }

    public void testStart_InitializesTheSender() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        try (var service = new TestSenderService(factory, createWithEmptySettings(threadPool))) {
            PlainActionFuture<Boolean> listener = new PlainActionFuture<>();
            service.start(mock(Model.class), listener);

            listener.actionGet(TIMEOUT);
            verify(sender, times(1)).start();
            verify(factory, times(1)).createSender();
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testStart_CallingStartTwiceKeepsSameSenderReference() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        try (var service = new TestSenderService(factory, createWithEmptySettings(threadPool))) {
            PlainActionFuture<Boolean> listener = new PlainActionFuture<>();
            service.start(mock(Model.class), listener);
            listener.actionGet(TIMEOUT);

            service.start(mock(Model.class), listener);
            listener.actionGet(TIMEOUT);

            verify(factory, times(1)).createSender();
            verify(sender, times(2)).start();
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testReturnsValidationException_WhenQueryIsNullForRerankTaskType() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        try (var testService = new TestSenderService(factory, createWithEmptySettings(threadPool))) {
            var model = mock(Model.class);
            when(model.getTaskType()).thenReturn(TaskType.RERANK);

            var exception = expectThrows(
                ValidationException.class,
                () -> testService.infer(
                    model,
                    null,
                    null,
                    null,
                    List.of("test input"),
                    false,
                    Map.of(),
                    InputType.SEARCH,
                    null,
                    new PlainActionFuture<>()
                )
            );

            assertThat(exception.getMessage(), containsString("Rerank task type requires a non-null query field"));
        }
    }

    public void testInferSucceeds_WhenQueryIsDefinedForRerankTaskType() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var queryString = "a query";
        var testInput = "test input";
        var doInferCalled = new AtomicReference<>(false);

        var testService = new TestSenderService(factory, createWithEmptySettings(threadPool)) {
            @Override
            protected void doInfer(
                Model model,
                InferenceInputs inputs,
                Map<String, Object> taskSettings,
                TimeValue timeout,
                ActionListener<InferenceServiceResults> listener
            ) {
                var queryDocs = inputs.castTo(QueryAndDocsInputs.class);
                assertThat(queryDocs.getQuery(), is(queryString));
                assertThat(queryDocs.getChunks(), is(List.of(testInput)));
                doInferCalled.set(true);
                listener.onResponse(mock(InferenceServiceResults.class));
            }
        };

        try (testService) {
            var model = mock(Model.class);
            when(model.getTaskType()).thenReturn(TaskType.RERANK);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();

            testService.infer(model, queryString, null, null, List.of(testInput), false, Map.of(), null, null, listener);
            assertNotNull(listener.actionGet(TIMEOUT));
            assertTrue(doInferCalled.get());
        }
    }

    public void testInferSucceeds_WhenQueryIsNotDefinedForCompletionTaskType() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender()).thenReturn(sender);

        var testInput = "test input";
        var doInferCalled = new AtomicReference<>(false);

        var testService = new TestSenderService(factory, createWithEmptySettings(threadPool)) {
            @Override
            protected void doInfer(
                Model model,
                InferenceInputs inputs,
                Map<String, Object> taskSettings,
                TimeValue timeout,
                ActionListener<InferenceServiceResults> listener
            ) {
                var castedInput = inputs.castTo(ChatCompletionInput.class);
                assertThat(castedInput.getInputs(), is(List.of(testInput)));
                doInferCalled.set(true);
                listener.onResponse(mock(InferenceServiceResults.class));
            }
        };

        try (testService) {
            var model = mock(Model.class);
            when(model.getTaskType()).thenReturn(TaskType.COMPLETION);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();

            testService.infer(model, null, null, null, List.of(testInput), false, Map.of(), null, null, listener);
            assertNotNull(listener.actionGet(TIMEOUT));
            assertTrue(doInferCalled.get());
        }
    }

    private static class TestSenderService extends SenderService {
        TestSenderService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents) {
            super(factory, serviceComponents);
        }

        @Override
        protected void doInfer(
            Model model,
            InferenceInputs inputs,
            Map<String, Object> taskSettings,
            TimeValue timeout,
            ActionListener<InferenceServiceResults> listener
        ) {

        }

        @Override
        protected void validateInputType(InputType inputType, Model model, ValidationException validationException) {}

        @Override
        protected void doUnifiedCompletionInfer(
            Model model,
            UnifiedChatInput inputs,
            TimeValue timeout,
            ActionListener<InferenceServiceResults> listener
        ) {}

        @Override
        protected void doChunkedInfer(
            Model model,
            List<ChunkInferenceInput> inputs,
            Map<String, Object> taskSettings,
            InputType inputType,
            TimeValue timeout,
            ActionListener<List<ChunkedInference>> listener
        ) {

        }

        @Override
        public String name() {
            return "test service";
        }

        @Override
        public void parseRequestConfig(
            String inferenceEntityId,
            TaskType taskType,
            Map<String, Object> config,
            ActionListener<Model> parsedModelListener
        ) {
            parsedModelListener.onResponse(null);
        }

        @Override
        public Model parsePersistedConfigWithSecrets(
            String inferenceEntityId,
            TaskType taskType,
            Map<String, Object> config,
            Map<String, Object> secrets
        ) {
            return null;
        }

        @Override
        public Model parsePersistedConfig(String inferenceEntityId, TaskType taskType, Map<String, Object> config) {
            return null;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public InferenceServiceConfiguration getConfiguration() {
            return new InferenceServiceConfiguration.Builder().setService("test service")
                .setName("Test")
                .setTaskTypes(supportedTaskTypes())
                .setConfigurations(new HashMap<>())
                .build();
        }

        @Override
        public EnumSet<TaskType> supportedTaskTypes() {
            return EnumSet.of(TaskType.TEXT_EMBEDDING);
        }
    }
}
