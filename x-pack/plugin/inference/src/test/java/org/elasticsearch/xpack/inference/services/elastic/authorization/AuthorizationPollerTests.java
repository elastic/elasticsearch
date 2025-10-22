/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.registry.StoreInferenceEndpointsAction;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints;
import org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntity;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AuthorizationPollerTests extends ESTestCase {
    private DeterministicTaskQueue taskQueue;

    @Before
    public void init() throws Exception {
        taskQueue = new DeterministicTaskQueue();
    }

    public void testDoesNotSendAuthorizationRequest_WhenModelRegistryIsNotReady() {
        var mockRegistry = mock(ModelRegistry.class);
        when(mockRegistry.isReady()).thenReturn(false);

        var authorizationRequestHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);

        var poller = new AuthorizationPoller(
            new AuthorizationPoller.TaskFields(0, "abc", "abc", "abc", new TaskId("abc", 0), Map.of()),
            createWithEmptySettings(taskQueue.getThreadPool()),
            authorizationRequestHandler,
            mock(Sender.class),
            ElasticInferenceServiceSettingsTests.create(null, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
            new ElasticInferenceServiceComponents(""),
            mockRegistry,
            mock(Client.class),
            null
        );

        poller.sendAuthorizationRequest();

        verify(authorizationRequestHandler, never()).getAuthorization(any(), any());
    }

    public void testSendsAuthorizationRequest_WhenModelRegistryIsReady() {
        var mockRegistry = mock(ModelRegistry.class);
        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getInferenceIds()).thenReturn(Set.of("id1", "id2"));

        var mockAuthHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);
        doAnswer(invocation -> {
            ActionListener<ElasticInferenceServiceAuthorizationModel> listener = invocation.getArgument(0);
            listener.onResponse(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(
                        List.of(
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                InternalPreconfiguredEndpoints.DEFAULT_ELSER_2_MODEL_ID,
                                EnumSet.of(TaskType.SPARSE_EMBEDDING)
                            )
                        )
                    )
                )
            );
            return Void.TYPE;
        }).when(mockAuthHandler).getAuthorization(any(), any());

        var mockClient = mock(Client.class);
        when(mockClient.threadPool()).thenReturn(taskQueue.getThreadPool());

        var eisComponents = new ElasticInferenceServiceComponents("");

        var poller = new AuthorizationPoller(
            new AuthorizationPoller.TaskFields(0, "abc", "abc", "abc", new TaskId("abc", 0), Map.of()),
            createWithEmptySettings(taskQueue.getThreadPool()),
            mockAuthHandler,
            mock(Sender.class),
            ElasticInferenceServiceSettingsTests.create(null, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
            eisComponents,
            mockRegistry,
            mockClient,
            null
        );

        var requestArgCaptor = ArgumentCaptor.forClass(StoreInferenceEndpointsAction.Request.class);

        poller.sendAuthorizationRequest();
        verify(mockClient).execute(eq(StoreInferenceEndpointsAction.INSTANCE), requestArgCaptor.capture(), any());
        var capturedRequest = requestArgCaptor.getValue();
        assertThat(
            capturedRequest.getModels(),
            is(
                List.of(
                    PreconfiguredEndpointModelAdapter.createModel(
                        InternalPreconfiguredEndpoints.getWithInferenceId(InternalPreconfiguredEndpoints.DEFAULT_ELSER_ENDPOINT_ID_V2),
                        eisComponents
                    )
                )
            )
        );
    }

    public void testDoesNotAttemptToStoreModelIds_ThatDoNotExistInThePreconfiguredMapping() {
        var mockRegistry = mock(ModelRegistry.class);
        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getInferenceIds()).thenReturn(Set.of("id1", "id2"));

        var mockAuthHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);
        doAnswer(invocation -> {
            ActionListener<ElasticInferenceServiceAuthorizationModel> listener = invocation.getArgument(0);
            listener.onResponse(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(
                        List.of(
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                // This is a model id that does not exist in the preconfigured endpoints map so it will not be stored
                                "abc",
                                EnumSet.of(TaskType.SPARSE_EMBEDDING)
                            )
                        )
                    )
                )
            );
            return Void.TYPE;
        }).when(mockAuthHandler).getAuthorization(any(), any());

        var mockClient = mock(Client.class);
        when(mockClient.threadPool()).thenReturn(taskQueue.getThreadPool());

        var eisComponents = new ElasticInferenceServiceComponents("");

        var poller = new AuthorizationPoller(
            new AuthorizationPoller.TaskFields(0, "abc", "abc", "abc", new TaskId("abc", 0), Map.of()),
            createWithEmptySettings(taskQueue.getThreadPool()),
            mockAuthHandler,
            mock(Sender.class),
            ElasticInferenceServiceSettingsTests.create(null, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
            eisComponents,
            mockRegistry,
            mockClient,
            null
        );

        var requestArgCaptor = ArgumentCaptor.forClass(StoreInferenceEndpointsAction.Request.class);

        poller.sendAuthorizationRequest();
        verify(mockClient, never()).execute(eq(StoreInferenceEndpointsAction.INSTANCE), requestArgCaptor.capture(), any());
    }

    public void testDoesNotAttemptToStoreModelIds_ThatHaveATaskTypeThatTheEISIntegration_DoesNotSupport() {
        var mockRegistry = mock(ModelRegistry.class);
        when(mockRegistry.isReady()).thenReturn(true);
        when(mockRegistry.getInferenceIds()).thenReturn(Set.of("id1", "id2"));

        var mockAuthHandler = mock(ElasticInferenceServiceAuthorizationRequestHandler.class);
        doAnswer(invocation -> {
            ActionListener<ElasticInferenceServiceAuthorizationModel> listener = invocation.getArgument(0);
            listener.onResponse(
                ElasticInferenceServiceAuthorizationModel.of(
                    new ElasticInferenceServiceAuthorizationResponseEntity(
                        List.of(
                            new ElasticInferenceServiceAuthorizationResponseEntity.AuthorizedModel(
                                InternalPreconfiguredEndpoints.DEFAULT_ELSER_2_MODEL_ID,
                                // EIS does not yet support completions so this model will be ignored
                                EnumSet.of(TaskType.COMPLETION)
                            )
                        )
                    )
                )
            );
            return Void.TYPE;
        }).when(mockAuthHandler).getAuthorization(any(), any());

        var mockClient = mock(Client.class);
        when(mockClient.threadPool()).thenReturn(taskQueue.getThreadPool());

        var eisComponents = new ElasticInferenceServiceComponents("");

        var poller = new AuthorizationPoller(
            new AuthorizationPoller.TaskFields(0, "abc", "abc", "abc", new TaskId("abc", 0), Map.of()),
            createWithEmptySettings(taskQueue.getThreadPool()),
            mockAuthHandler,
            mock(Sender.class),
            ElasticInferenceServiceSettingsTests.create(null, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
            eisComponents,
            mockRegistry,
            mockClient,
            null
        );

        var requestArgCaptor = ArgumentCaptor.forClass(StoreInferenceEndpointsAction.Request.class);

        poller.sendAuthorizationRequest();
        verify(mockClient, never()).execute(eq(StoreInferenceEndpointsAction.INSTANCE), requestArgCaptor.capture(), any());
    }

    public void testSendsTwoAuthorizationRequests() {
        fail("TODO");
    }
}
