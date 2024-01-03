/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.batching.OpenAiRequestBatcherFactory;
import org.elasticsearch.xpack.inference.external.http.sender.InferenceRequestSenderFactory;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.openai.OpenAiAccount;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.mockito.ArgumentMatchers.any;
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

    @SuppressWarnings("unchecked")
    public void testStart_InitializesTheSender() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(InferenceRequestSenderFactory.class);
        when(factory.createSender(anyString(), any())).thenReturn(sender);

        try (var service = new TestSenderService(new SetOnce<>(factory), new SetOnce<>(createWithEmptySettings(threadPool)))) {
            PlainActionFuture<Boolean> listener = new PlainActionFuture<>();
            service.start(mock(Model.class), listener);

            listener.actionGet(TIMEOUT);
            verify(sender, times(1)).start();
            verify(factory, times(1)).createSender(anyString(), any());
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    @SuppressWarnings("unchecked")
    public void testStart_CallingStartTwiceKeepsSameSenderReference() throws IOException {
        var sender = mock(Sender.class);

        var factory = mock(InferenceRequestSenderFactory.class);
        when(factory.createSender(anyString(), any())).thenReturn(sender);

        try (var service = new TestSenderService(new SetOnce<>(factory), new SetOnce<>(createWithEmptySettings(threadPool)))) {
            PlainActionFuture<Boolean> listener = new PlainActionFuture<>();
            service.start(mock(Model.class), listener);
            listener.actionGet(TIMEOUT);

            service.start(mock(Model.class), listener);
            listener.actionGet(TIMEOUT);

            verify(factory, times(1)).createSender(anyString(), any());
            verify(sender, times(2)).start();
        }

        verify(sender, times(1)).close();
        verifyNoMoreInteractions(factory);
        verifyNoMoreInteractions(sender);
    }

    private static final class TestSenderService extends SenderService<OpenAiAccount> {
        TestSenderService(SetOnce<InferenceRequestSenderFactory> factory, SetOnce<ServiceComponents> serviceComponents) {
            super(factory, serviceComponents, OpenAiRequestBatcherFactory::new);
        }

        @Override
        protected void doInfer(
            Model model,
            List<String> input,
            Map<String, Object> taskSettings,
            ActionListener<InferenceServiceResults> listener
        ) {

        }

        @Override
        public String name() {
            return "test service";
        }

        @Override
        public Model parseRequestConfig(String modelId, TaskType taskType, Map<String, Object> config, Set<String> platfromArchitectures) {
            return null;
        }

        @Override
        public Model parsePersistedConfigWithSecrets(
            String modelId,
            TaskType taskType,
            Map<String, Object> config,
            Map<String, Object> secrets
        ) {
            return null;
        }

        @Override
        public Model parsePersistedConfig(String modelId, TaskType taskType, Map<String, Object> config) {
            return null;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }
    }
}
