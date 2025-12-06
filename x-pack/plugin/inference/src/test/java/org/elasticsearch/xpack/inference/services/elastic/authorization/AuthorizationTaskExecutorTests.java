/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.authorization;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.persistent.ClusterPersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettingsTests;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import static org.elasticsearch.cluster.metadata.Metadata.EMPTY_METADATA;
import static org.elasticsearch.persistent.PersistentTasksExecutor.NO_NODE_FOUND;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeatureTests.createMockCCMFeature;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMServiceTests.createMockCCMService;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AuthorizationTaskExecutorTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private PersistentTasksService persistentTasksService;
    private String localNodeId;
    private FeatureService enabledFeatureServiceMock;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = createThreadPool(inferenceUtilityExecutors());
        clusterService = createClusterService(threadPool);
        persistentTasksService = mock(PersistentTasksService.class);
        localNodeId = clusterService.localNode().getId();
        enabledFeatureServiceMock = mock(FeatureService.class);
        when(enabledFeatureServiceMock.clusterHasFeature(any(), any())).thenReturn(true);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        terminate(threadPool);
    }

    public void testMultipleCallsToStart_OnlyRegistersOnce() {
        var eisUrl = "abc";
        var mockClusterService = createMockEmptyClusterService();
        var executor = new AuthorizationTaskExecutor(
            mockClusterService,
            persistentTasksService,
            enabledFeatureServiceMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(eisUrl, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false)
            )
        );
        executor.startAndImmediatelyCreateTask();
        executor.startAndImmediatelyCreateTask();

        verify(mockClusterService, times(1)).addListener(executor);
        verify(persistentTasksService, times(1)).sendClusterStartRequest(
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationTaskParams.INSTANCE),
            any(),
            any()
        );
    }

    public void testStartLazy_OnlyRegistersOnce_NeverCallsPersistentTaskService() {
        var eisUrl = "abc";
        var mockClusterService = createMockEmptyClusterService();
        var executor = new AuthorizationTaskExecutor(
            mockClusterService,
            persistentTasksService,
            enabledFeatureServiceMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(eisUrl, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false)
            )
        );
        executor.startAndLazyCreateTask();
        executor.startAndLazyCreateTask();

        verify(mockClusterService, times(1)).addListener(executor);
        verify(persistentTasksService, never()).sendClusterStartRequest(
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationTaskParams.INSTANCE),
            any(),
            any()
        );
    }

    private static ClusterService createMockEmptyClusterService() {
        var mockClusterService = mock(ClusterService.class);
        when(mockClusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        return mockClusterService;
    }

    public void testDoesNotRegisterListener_IfUrlIsEmpty() {
        var eisUrl = "";
        var mockClusterService = createMockEmptyClusterService();
        var executor = new AuthorizationTaskExecutor(
            mockClusterService,
            persistentTasksService,
            enabledFeatureServiceMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(eisUrl, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false)
            )
        );
        executor.startAndImmediatelyCreateTask();
        executor.startAndImmediatelyCreateTask();

        verify(mockClusterService, never()).addListener(executor);
        verify(persistentTasksService, never()).sendClusterStartRequest(
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationTaskParams.INSTANCE),
            any(),
            any()
        );
    }

    public void testMultipleCallsToStart_AndStop() {
        var eisUrl = "abc";
        var mockClusterService = createMockEmptyClusterService();
        var executor = new AuthorizationTaskExecutor(
            mockClusterService,
            persistentTasksService,
            enabledFeatureServiceMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(eisUrl, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false)
            )
        );
        executor.startAndImmediatelyCreateTask();
        executor.startAndImmediatelyCreateTask();
        executor.stop();
        executor.stop();
        verify(mockClusterService, times(1)).addListener(executor);
        verify(persistentTasksService, times(1)).sendClusterStartRequest(
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationTaskParams.INSTANCE),
            any(),
            any()
        );
        verify(mockClusterService, times(1)).removeListener(executor);
        verify(persistentTasksService, times(1)).sendClusterRemoveRequest(eq(AuthorizationPoller.TASK_NAME), any(), any());

        executor.startAndImmediatelyCreateTask();
        executor.stop();
        verify(mockClusterService, times(2)).addListener(executor);
        verify(persistentTasksService, times(2)).sendClusterStartRequest(
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationTaskParams.INSTANCE),
            any(),
            any()
        );
        verify(mockClusterService, times(2)).removeListener(executor);
        verify(persistentTasksService, times(2)).sendClusterRemoveRequest(eq(AuthorizationPoller.TASK_NAME), any(), any());
    }

    public void testCallsSendClusterStartRequest_WhenStartIsCalled() {
        var eisUrl = "abc";
        var mockClusterService = createMockEmptyClusterService();
        var executor = new AuthorizationTaskExecutor(
            mockClusterService,
            persistentTasksService,
            enabledFeatureServiceMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(eisUrl, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false)
            )
        );
        executor.startAndImmediatelyCreateTask();

        verify(mockClusterService, times(1)).addListener(executor);
        verify(persistentTasksService, times(1)).sendClusterStartRequest(
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationTaskParams.INSTANCE),
            any(),
            any()
        );
    }

    public void testDoesNotCallSendClusterStartRequest_WhenStartIsCalled_WhenItIsAlreadyInClusterState() {
        var initialState = initialState();
        var state = ClusterState.builder(initialState)
            .metadata(
                Metadata.builder(initialState.metadata())
                    .putCustom(
                        ClusterPersistentTasksCustomMetadata.TYPE,
                        ClusterPersistentTasksCustomMetadata.builder()
                            .addTask(
                                AuthorizationPoller.TASK_NAME,
                                AuthorizationPoller.TASK_NAME,
                                AuthorizationTaskParams.INSTANCE,
                                NO_NODE_FOUND
                            )
                            .build()
                    )
            )
            .build();

        var mockClusterService = mock(ClusterService.class);
        when(mockClusterService.state()).thenReturn(state);

        var eisUrl = "abc";
        var executor = new AuthorizationTaskExecutor(
            mockClusterService,
            persistentTasksService,
            enabledFeatureServiceMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(eisUrl, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false)
            )
        );
        executor.startAndImmediatelyCreateTask();

        verify(mockClusterService, times(1)).addListener(executor);
        verify(persistentTasksService, never()).sendClusterStartRequest(
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationTaskParams.INSTANCE),
            any(),
            any()
        );
    }

    public void testCreatesTask_WhenItDoesNotExistOnClusterStateChange() {
        var eisUrl = "abc";

        var executor = new AuthorizationTaskExecutor(
            clusterService,
            persistentTasksService,
            enabledFeatureServiceMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(eisUrl, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false)
            )
        );
        executor.startAndImmediatelyCreateTask();

        var listener1 = new PlainActionFuture<Void>();
        clusterService.getClusterApplierService().onNewClusterState("initialization", this::initialState, listener1);
        listener1.actionGet(TimeValue.THIRTY_SECONDS);
        // 2 because the first call is from the start() and the second is from the cluster state change.
        verify(persistentTasksService, times(2)).sendClusterStartRequest(
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationTaskParams.INSTANCE),
            any(),
            any()
        );

        Mockito.clearInvocations(persistentTasksService);
        // Ensure that if the task is gone, it will be recreated.
        var listener2 = new PlainActionFuture<Void>();
        clusterService.getClusterApplierService().onNewClusterState("initialization", this::initialState, listener2);
        listener2.actionGet(TimeValue.THIRTY_SECONDS);
        verify(persistentTasksService, times(1)).sendClusterStartRequest(
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationPoller.TASK_NAME),
            eq(new AuthorizationTaskParams()),
            any(),
            any()
        );
    }

    private ClusterState initialState() {
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create(localNodeId))
            .localNodeId(localNodeId)
            .masterNodeId(localNodeId);

        return ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).metadata(EMPTY_METADATA).build();
    }

    public void testDoesNotCreateTask_WhenFeatureIsNotSupported() {
        var eisUrl = "abc";
        var disabledFeatureServiceMock = mock(FeatureService.class);
        when(disabledFeatureServiceMock.clusterHasFeature(any(), any())).thenReturn(false);

        var executor = new AuthorizationTaskExecutor(
            clusterService,
            persistentTasksService,
            disabledFeatureServiceMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(eisUrl, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false)
            )
        );
        executor.startAndImmediatelyCreateTask();

        var listener1 = new PlainActionFuture<Void>();
        clusterService.getClusterApplierService().onNewClusterState("initialization", this::initialState, listener1);
        listener1.actionGet(TimeValue.THIRTY_SECONDS);
        // We should never call sendClusterStartRequest since the feature is not supported
        verify(persistentTasksService, never()).sendClusterStartRequest(
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationTaskParams.INSTANCE),
            any(),
            any()
        );
    }

    public void testDoesNotRegisterClusterStateListener_DoesNotCreateTask_WhenTheEisUrlIsEmpty() {
        var executor = new AuthorizationTaskExecutor(
            clusterService,
            persistentTasksService,
            enabledFeatureServiceMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create("", TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false)
            )
        );
        executor.startAndImmediatelyCreateTask();

        var listener = new PlainActionFuture<Void>();
        clusterService.getClusterApplierService().onNewClusterState("initialization", this::initialState, listener);
        listener.actionGet(TimeValue.THIRTY_SECONDS);
        verify(persistentTasksService, never()).sendClusterStartRequest(
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationTaskParams.INSTANCE),
            any(),
            any()
        );
    }

    public void testDoesNotRegisterClusterStateListener_DoesNotCreateTask_WhenTheEisUrlIsNull() {
        var executor = new AuthorizationTaskExecutor(
            clusterService,
            persistentTasksService,
            enabledFeatureServiceMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(null, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false)
            )
        );
        executor.startAndImmediatelyCreateTask();

        var listener = new PlainActionFuture<Void>();
        clusterService.getClusterApplierService().onNewClusterState("initialization", this::initialState, listener);
        listener.actionGet(TimeValue.THIRTY_SECONDS);
        verify(persistentTasksService, never()).sendClusterStartRequest(
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationTaskParams.INSTANCE),
            any(),
            any()
        );
    }

    public void testDoesNotCreateTask_OnClusterStateChange_WhenItAlreadyExists() {
        var initialState = initialState();
        var state = ClusterState.builder(initialState)
            .metadata(
                Metadata.builder(initialState.metadata())
                    .putCustom(
                        ClusterPersistentTasksCustomMetadata.TYPE,
                        ClusterPersistentTasksCustomMetadata.builder()
                            .addTask(
                                AuthorizationPoller.TASK_NAME,
                                AuthorizationPoller.TASK_NAME,
                                AuthorizationTaskParams.INSTANCE,
                                NO_NODE_FOUND
                            )
                            .build()
                    )
            )
            .build();
        var event = new ClusterChangedEvent("testClusterChanged", state, ClusterState.EMPTY_STATE);

        var mockClusterService = mock(ClusterService.class);
        when(mockClusterService.state()).thenReturn(state);

        var eisUrl = "abc";
        var executor = new AuthorizationTaskExecutor(
            mockClusterService,
            persistentTasksService,
            enabledFeatureServiceMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(eisUrl, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false)
            )
        );
        executor.startAndImmediatelyCreateTask();

        executor.clusterChanged(event);
        verify(persistentTasksService, never()).sendClusterStartRequest(
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationTaskParams.INSTANCE),
            any(),
            any()
        );
    }
}
