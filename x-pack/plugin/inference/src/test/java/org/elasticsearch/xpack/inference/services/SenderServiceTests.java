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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.ChunkedInferenceServiceResults;
import org.elasticsearch.inference.ChunkingOptions;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.mockito.ArgumentMatchers.anyString;
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
        when(factory.createSender(anyString())).thenReturn(sender);

        try (var service = new TestSenderService(factory, createWithEmptySettings(threadPool))) {
            PlainActionFuture<Boolean> listener = new PlainActionFuture<>();
            service.start(mock(Model.class), listener);

            listener.actionGet(TIMEOUT);
            verify(sender, times(1)).start();
            verify(factory, times(1)).createSender(anyString());
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    public void testStart_CallingStartTwiceKeepsSameSenderReference() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(HttpRequestSender.Factory.class);
        when(factory.createSender(anyString())).thenReturn(sender);

        try (var service = new TestSenderService(factory, createWithEmptySettings(threadPool))) {
            PlainActionFuture<Boolean> listener = new PlainActionFuture<>();
            service.start(mock(Model.class), listener);
            listener.actionGet(TIMEOUT);

            service.start(mock(Model.class), listener);
            listener.actionGet(TIMEOUT);

            verify(factory, times(1)).createSender(anyString());
            verify(sender, times(2)).start();
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    private static final class TestSenderService extends SenderService {
        TestSenderService(HttpRequestSender.Factory factory, ServiceComponents serviceComponents) {
            super(factory, serviceComponents);
        }

        @Override
        protected void doInfer(
            Model model,
            List<String> input,
            Map<String, Object> taskSettings,
            InputType inputType,
            ActionListener<InferenceServiceResults> listener
        ) {

        }

        @Override
        protected void doChunkedInfer(
            Model model,
            List<String> input,
            Map<String, Object> taskSettings,
            InputType inputType,
            ChunkingOptions chunkingOptions,
            ActionListener<List<ChunkedInferenceServiceResults>> listener
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
            Set<String> platfromArchitectures,
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
    }
}
