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
import org.elasticsearch.xpack.inference.features.InferenceFeatureService;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettingsTests;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMEnablementService;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Duration;

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

    private static final String EIS_FAKE_URL = "abc";

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private PersistentTasksService persistentTasksService;
    private String localNodeId;
    private FeatureService enabledFeatureServiceMock;
    private InferenceFeatureService inferenceFeatureService;
    private CCMFeature unsupportedEnvironmentCcmFeatureMock;
    private CCMEnablementService ccmEnablementServiceMock;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = createThreadPool(inferenceUtilityExecutors());
        clusterService = createClusterService(threadPool);
        persistentTasksService = mock(PersistentTasksService.class);
        localNodeId = clusterService.localNode().getId();
        enabledFeatureServiceMock = mock(FeatureService.class);
        when(enabledFeatureServiceMock.clusterHasFeature(any(), any())).thenReturn(true);
        inferenceFeatureService = new InferenceFeatureService(clusterService, enabledFeatureServiceMock);
        unsupportedEnvironmentCcmFeatureMock = mock(CCMFeature.class);
        when(unsupportedEnvironmentCcmFeatureMock.isCcmSupportedEnvironment()).thenReturn(false);
        ccmEnablementServiceMock = mock(CCMEnablementService.class);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        terminate(threadPool);
    }

    public void testStartLazy_OnlyRegistersClusterStateListenerOnce() {
        var mockClusterService = createMockEmptyClusterService();
        var executor = new AuthorizationTaskExecutor(
            mockClusterService,
            persistentTasksService,
            enabledFeatureServiceMock,
            ccmEnablementServiceMock,
            unsupportedEnvironmentCcmFeatureMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(EIS_FAKE_URL, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false),
                inferenceFeatureService
            ),
            Clock.systemUTC()
        );
        executor.startAndLazilyCreateTask();
        executor.startAndLazilyCreateTask();

        verify(mockClusterService, times(1)).addListener(executor);
    }

    public void testStartLazy_AttemptsToCreateTask_WhenNotInCCMSupportedEnvironment_OnlyOnce() {
        var executor = new AuthorizationTaskExecutor(
            clusterService,
            persistentTasksService,
            enabledFeatureServiceMock,
            ccmEnablementServiceMock,
            unsupportedEnvironmentCcmFeatureMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(EIS_FAKE_URL, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false),
                inferenceFeatureService
            ),
            Clock.systemUTC()
        );
        executor.startAndLazilyCreateTask();
        executor.startAndLazilyCreateTask();

        var listener = new PlainActionFuture<Void>();
        clusterService.getClusterApplierService().onNewClusterState("initialization", this::initialState, listener);
        listener.actionGet(TimeValue.THIRTY_SECONDS);

        verify(persistentTasksService, times(1)).sendClusterStartRequest(
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
            ccmEnablementServiceMock,
            unsupportedEnvironmentCcmFeatureMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(eisUrl, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false),
                inferenceFeatureService
            ),
            Clock.systemUTC()
        );
        executor.startAndLazilyCreateTask();
        executor.startAndLazilyCreateTask();

        verify(mockClusterService, never()).addListener(executor);
        verify(persistentTasksService, never()).sendClusterStartRequest(
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationTaskParams.INSTANCE),
            any(),
            any()
        );
    }

    public void testMultipleCallsToStart_OnlyCallsSendClusterStartRequestOnce_WhenRateLimited() {
        var now = Clock.systemUTC().instant();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var executor = new AuthorizationTaskExecutor(
            clusterService,
            persistentTasksService,
            enabledFeatureServiceMock,
            ccmEnablementServiceMock,
            unsupportedEnvironmentCcmFeatureMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(EIS_FAKE_URL, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false),
                inferenceFeatureService
            ),
            clock
        );

        executor.startAndLazilyCreateTask();
        executor.startAndLazilyCreateTask();

        var listener = new PlainActionFuture<Void>();
        clusterService.getClusterApplierService().onNewClusterState("initialization", this::initialState, listener);
        listener.actionGet(TimeValue.THIRTY_SECONDS);

        verify(persistentTasksService, times(1)).sendClusterStartRequest(
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationTaskParams.INSTANCE),
            any(),
            any()
        );

        Mockito.clearInvocations(persistentTasksService);

        executor.sendStartRequestWithCurrentClusterState();
        // No additional calls because time hasn't advanced to allow another task creation call
        verify(persistentTasksService, never()).sendClusterStartRequest(
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationTaskParams.INSTANCE),
            any(),
            any()
        );
    }

    public void testMultipleCallsToStart_OnlyCallsSendClusterStartRequestOnce_WhenRateLimitExpired() {
        var now = Clock.systemUTC().instant();
        var oneDayInFuture = now.plus(Duration.ofDays(1));
        var clock = mock(Clock.class);
        // The AuthorizationTaskExecutor does these calls:
        // 1. Check if the last create task time is expired (first call to instant()),
        // this will pass so a call to create the task will occur
        // 2. Then it will update the last create task time (second call to instant())
        // 3. On the next cluster state change, it will check if the last create task time is expired (third call to instant()),
        // we'll return now + 1 day to ensure that it is expired and allows another call to create the task
        when(clock.instant()).thenReturn(now).thenReturn(now).thenReturn(oneDayInFuture);

        var executor = new AuthorizationTaskExecutor(
            clusterService,
            persistentTasksService,
            enabledFeatureServiceMock,
            ccmEnablementServiceMock,
            unsupportedEnvironmentCcmFeatureMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(EIS_FAKE_URL, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false),
                inferenceFeatureService
            ),
            clock
        );

        executor.startAndLazilyCreateTask();
        executor.startAndLazilyCreateTask();

        var listener = new PlainActionFuture<Void>();
        clusterService.getClusterApplierService().onNewClusterState("initialization", this::initialState, listener);
        listener.actionGet(TimeValue.THIRTY_SECONDS);

        verify(persistentTasksService, times(1)).sendClusterStartRequest(
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationTaskParams.INSTANCE),
            any(),
            any()
        );

        Mockito.clearInvocations(persistentTasksService);

        executor.sendStartRequestWithCurrentClusterState();
        // Time has advanced so we should have another call
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

        var executor = new AuthorizationTaskExecutor(
            mockClusterService,
            persistentTasksService,
            enabledFeatureServiceMock,
            ccmEnablementServiceMock,
            unsupportedEnvironmentCcmFeatureMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(EIS_FAKE_URL, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false),
                inferenceFeatureService
            ),
            Clock.systemUTC()
        );
        executor.startAndLazilyCreateTask();

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
        var now = Clock.systemUTC().instant();
        var oneDayInFuture = now.plus(Duration.ofDays(1));
        var clock = mock(Clock.class);
        // The AuthorizationTaskExecutor does these calls:
        // 1. Check if the last create task time is expired (first call to instant()),
        // this will pass so a call to create the task will occur
        // 2. Then it will update the last create task time (second call to instant())
        // 3. On the next cluster state change, it will check if the last create task time is expired (third call to instant()),
        // we'll return now + 1 day to ensure that it is expired and allows another call to create the task
        when(clock.instant()).thenReturn(now).thenReturn(now).thenReturn(oneDayInFuture);

        var executor = new AuthorizationTaskExecutor(
            clusterService,
            persistentTasksService,
            enabledFeatureServiceMock,
            ccmEnablementServiceMock,
            unsupportedEnvironmentCcmFeatureMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(EIS_FAKE_URL, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false),
                inferenceFeatureService
            ),
            clock
        );
        executor.startAndLazilyCreateTask();

        var listener1 = new PlainActionFuture<Void>();
        clusterService.getClusterApplierService().onNewClusterState("initialization", this::initialState, listener1);
        listener1.actionGet(TimeValue.THIRTY_SECONDS);
        verify(persistentTasksService, times(1)).sendClusterStartRequest(
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationTaskParams.INSTANCE),
            any(),
            any()
        );

        Mockito.clearInvocations(persistentTasksService);
        Mockito.clearInvocations(clock);
        when(clock.instant()).thenReturn(oneDayInFuture.plus(Duration.ofDays(1)));

        // Ensure that if the task is gone, it will be recreated.
        var listener2 = new PlainActionFuture<Void>();
        clusterService.getClusterApplierService().onNewClusterState("initialization", this::initialState, listener2);
        listener2.actionGet(TimeValue.THIRTY_SECONDS);
        verify(persistentTasksService, times(1)).sendClusterStartRequest(
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationTaskParams.INSTANCE),
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

    public void testCreatesTask_WhenItDoesNotExistOnClusterStateChange_CcmSupportedAndConfigured() {
        var now = Clock.systemUTC().instant();
        var oneDayInFuture = now.plus(Duration.ofDays(1));
        var clock = mock(Clock.class);
        // The AuthorizationTaskExecutor does these calls:
        // 1. Check if the last create task time is expired (first call to instant()),
        // this will pass so a call to create the task will occur
        // 2. Then it will update the last create task time (second call to instant())
        // 3. On the next cluster state change, it will check if the last create task time is expired (third call to instant()),
        // we'll return now + 1 day to ensure that it is expired and allows another call to create the task
        when(clock.instant()).thenReturn(now).thenReturn(now).thenReturn(oneDayInFuture);

        var supportedEnvironmentCcmFeatureMock = mock(CCMFeature.class);
        when(supportedEnvironmentCcmFeatureMock.isCcmSupportedEnvironment()).thenReturn(true);
        when(ccmEnablementServiceMock.isEnabled(any())).thenReturn(true);

        var executor = new AuthorizationTaskExecutor(
            clusterService,
            persistentTasksService,
            enabledFeatureServiceMock,
            ccmEnablementServiceMock,
            supportedEnvironmentCcmFeatureMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(EIS_FAKE_URL, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false),
                inferenceFeatureService
            ),
            clock
        );
        executor.startAndLazilyCreateTask();

        var listener1 = new PlainActionFuture<Void>();
        clusterService.getClusterApplierService().onNewClusterState("initialization", this::initialState, listener1);
        listener1.actionGet(TimeValue.THIRTY_SECONDS);
        verify(persistentTasksService, times(1)).sendClusterStartRequest(
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
            eq(AuthorizationTaskParams.INSTANCE),
            any(),
            any()
        );
    }

    public void testDoesNotCreateTask_WhenCcmIsSupportedButNotEnabled() {
        var supportedEnvironmentCcmFeatureMock = mock(CCMFeature.class);
        when(supportedEnvironmentCcmFeatureMock.isCcmSupportedEnvironment()).thenReturn(true);
        // CCM is supported but not enabled
        when(ccmEnablementServiceMock.isEnabled(any())).thenReturn(false);

        var executor = new AuthorizationTaskExecutor(
            clusterService,
            persistentTasksService,
            enabledFeatureServiceMock,
            ccmEnablementServiceMock,
            supportedEnvironmentCcmFeatureMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(EIS_FAKE_URL, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false),
                inferenceFeatureService
            ),
            Clock.systemUTC()
        );
        executor.startAndLazilyCreateTask();

        var listener = new PlainActionFuture<Void>();
        clusterService.getClusterApplierService().onNewClusterState("initialization", this::initialState, listener);
        listener.actionGet(TimeValue.THIRTY_SECONDS);

        // Because CCM is supported but not enabled we should *not* create the task
        verify(persistentTasksService, never()).sendClusterStartRequest(
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationPoller.TASK_NAME),
            eq(AuthorizationTaskParams.INSTANCE),
            any(),
            any()
        );
    }

    public void testDoesNotCreateTask_WhenFeatureIsNotSupported() {
        var disabledFeatureServiceMock = mock(FeatureService.class);
        when(disabledFeatureServiceMock.clusterHasFeature(any(), any())).thenReturn(false);

        var executor = new AuthorizationTaskExecutor(
            clusterService,
            persistentTasksService,
            disabledFeatureServiceMock,
            ccmEnablementServiceMock,
            unsupportedEnvironmentCcmFeatureMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(EIS_FAKE_URL, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false),
                inferenceFeatureService
            ),
            Clock.systemUTC()
        );
        executor.startAndLazilyCreateTask();

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
            ccmEnablementServiceMock,
            unsupportedEnvironmentCcmFeatureMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create("", TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false),
                inferenceFeatureService
            ),
            Clock.systemUTC()
        );
        executor.startAndLazilyCreateTask();

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
            ccmEnablementServiceMock,
            unsupportedEnvironmentCcmFeatureMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(null, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false),
                inferenceFeatureService
            ),
            Clock.systemUTC()
        );
        executor.startAndLazilyCreateTask();

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

        var executor = new AuthorizationTaskExecutor(
            mockClusterService,
            persistentTasksService,
            enabledFeatureServiceMock,
            ccmEnablementServiceMock,
            unsupportedEnvironmentCcmFeatureMock,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(EIS_FAKE_URL, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class),
                createMockCCMFeature(false),
                createMockCCMService(false),
                inferenceFeatureService
            ),
            Clock.systemUTC()
        );
        executor.startAndLazilyCreateTask();

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
