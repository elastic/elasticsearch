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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AuthorizationTaskExecutorTests extends ESTestCase {

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private PersistentTasksService persistentTasksService;
    private String localNodeId;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = createThreadPool(inferenceUtilityExecutors());
        clusterService = createClusterService(threadPool);
        persistentTasksService = mock(PersistentTasksService.class);
        localNodeId = clusterService.localNode().getId();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        terminate(threadPool);
    }

    public void testCreatesTask_WhenItDoesNotExistOnClusterStateChange() {
        var eisUrl = "abc";

        var executor = new AuthorizationTaskExecutor(
            clusterService,
            persistentTasksService,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(eisUrl, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class)
            )
        );
        executor.init();

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

    public void testDoesNotRegisterClusterStateListener_DoesNotCreateTask_WhenTheEisUrlIsEmpty() {
        var executor = new AuthorizationTaskExecutor(
            clusterService,
            persistentTasksService,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create("", TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class)
            )
        );
        executor.init();

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
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(null, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class)
            )
        );
        executor.init();

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
        var event = new ClusterChangedEvent(
            "testClusterChanged",
            ClusterState.builder(initialState)
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
                .build(),
            ClusterState.EMPTY_STATE
        );

        var eisUrl = "abc";
        var executor = new AuthorizationTaskExecutor(
            clusterService,
            persistentTasksService,
            new AuthorizationPoller.Parameters(
                createWithEmptySettings(threadPool),
                mock(ElasticInferenceServiceAuthorizationRequestHandler.class),
                mock(Sender.class),
                ElasticInferenceServiceSettingsTests.create(eisUrl, TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), true),
                mock(ModelRegistry.class),
                mock(Client.class)
            )
        );
        executor.init();

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
