/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.persistent.PersistentTasksClusterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.action.SetUpgradeModeActionRequest;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMetadata;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.ActionTestUtils.assertNoFailureListener;
import static org.elasticsearch.xpack.core.action.AbstractTransportSetUpgradeModeActionTests.clusterService;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TransportSetTransformUpgradeModeActionTests extends ESTestCase {
    private ClusterService clusterService;
    private PersistentTasksService persistentTasksService;
    private Client client;
    private TransportSetTransformUpgradeModeAction action;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = clusterService();
        doAnswer(ans -> {
            ClusterStateUpdateTask task = ans.getArgument(1);
            task.clusterStateProcessed(ClusterState.EMPTY_STATE, ClusterState.EMPTY_STATE);
            return null;
        }).when(clusterService).submitUnbatchedStateUpdateTask(any(), any(ClusterStateUpdateTask.class));
        when(clusterService.getClusterSettings()).thenReturn(ClusterSettings.createBuiltInClusterSettings());
        PersistentTasksClusterService persistentTasksClusterService = new PersistentTasksClusterService(
            Settings.EMPTY,
            mock(),
            clusterService,
            mock()
        );
        persistentTasksService = mock();
        client = mock();
        ThreadPool threadPool = mock();
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadPool);
        action = new TransportSetTransformUpgradeModeAction(
            mock(),
            clusterService,
            threadPool,
            mock(),
            persistentTasksClusterService,
            persistentTasksService,
            client
        );
    }

    public void testUpgradeMode() {
        assertTrue(action.upgradeMode(state(true)));
        assertFalse(action.upgradeMode(state(false)));
    }

    private ClusterState state(boolean upgradeMode) {
        return ClusterState.EMPTY_STATE.copyAndUpdate(
            u -> u.metadata(
                ClusterState.EMPTY_STATE.metadata()
                    .copyAndUpdate(
                        b -> b.putCustom(
                            TransformMetadata.TYPE,
                            TransformMetadata.getTransformMetadata(ClusterState.EMPTY_STATE).builder().upgradeMode(upgradeMode).build()
                        )
                    )
            )
        );
    }

    public void testCreateUpdatedState() {
        var updatedState = action.createUpdatedState(new SetUpgradeModeActionRequest(true), state(false));
        assertTrue(TransformMetadata.upgradeMode(updatedState));

        updatedState = action.createUpdatedState(new SetUpgradeModeActionRequest(false), state(true));
        assertFalse(TransformMetadata.upgradeMode(updatedState));
    }

    public void testUpgradeModeWithNoMetadata() throws InterruptedException {
        upgradeModeSuccessfullyChanged(ClusterState.EMPTY_STATE, assertNoFailureListener(r -> {
            assertThat(r, is(AcknowledgedResponse.TRUE));
            verifyNoInteractions(persistentTasksService);
            verify(client, never()).admin();
        }));
    }

    public void testUpgradeModeWithNoTasks() throws InterruptedException {
        upgradeModeSuccessfullyChanged(
            ClusterState.EMPTY_STATE.copyAndUpdateMetadata(
                m -> m.putCustom(PersistentTasksCustomMetadata.TYPE, new PersistentTasksCustomMetadata(1, Map.of()))
            ),
            assertNoFailureListener(r -> {
                assertThat(r, is(AcknowledgedResponse.TRUE));
                verifyNoInteractions(persistentTasksService);
                verify(client, never()).admin();
            })
        );
    }

    public void testUpgradeModeWithNoTransformTasks() throws InterruptedException {
        upgradeModeSuccessfullyChanged(
            ClusterState.EMPTY_STATE.copyAndUpdateMetadata(
                m -> m.putCustom(
                    PersistentTasksCustomMetadata.TYPE,
                    new PersistentTasksCustomMetadata(1, Map.of("not a transform", mock()))
                )
            ),
            assertNoFailureListener(r -> {
                assertThat(r, is(AcknowledgedResponse.TRUE));
                verifyNoInteractions(persistentTasksService);
                verify(client, never()).admin();
            })
        );
    }

    private void upgradeModeSuccessfullyChanged(ClusterState state, ActionListener<AcknowledgedResponse> listener)
        throws InterruptedException {
        upgradeModeSuccessfullyChanged(new SetUpgradeModeActionRequest(true), state, listener);
    }

    private void upgradeModeSuccessfullyChanged(
        SetUpgradeModeActionRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        action.upgradeModeSuccessfullyChanged(mock(), request, state, ActionListener.runAfter(listener, latch::countDown));
        assertTrue("Failed to finish test after 10s", latch.await(10, TimeUnit.SECONDS));
    }

    public void testEnableUpgradeMode() throws InterruptedException {
        doAnswer(ans -> {
            ActionListener<ListTasksResponse> listener = ans.getArgument(2);
            ListTasksResponse response = mock();
            when(response.getNodeFailures()).thenReturn(List.of());
            listener.onResponse(response);
            return null;
        }).when(client).execute(any(), any(), any());

        when(client.admin()).thenReturn(new AdminClient(client));

        upgradeModeSuccessfullyChanged(stateWithTransformTask(), assertNoFailureListener(r -> {
            assertThat(r, is(AcknowledgedResponse.TRUE));
            verify(clusterService).submitUnbatchedStateUpdateTask(
                matches("unassign project .* persistent task \\[.*\\] from any node"),
                any()
            );
        }));
    }

    private ClusterState stateWithTransformTask() {
        PersistentTasksCustomMetadata.PersistentTask<?> task = mock();
        when(task.getTaskName()).thenReturn(TransformField.TASK_NAME);
        return ClusterState.EMPTY_STATE.copyAndUpdateMetadata(
            m -> m.putCustom(PersistentTasksCustomMetadata.TYPE, new PersistentTasksCustomMetadata(1, Map.of("a transform", task)))
        );
    }

    public void testDisableUpgradeMode() throws InterruptedException {
        doAnswer(ans -> {
            ActionListener<Boolean> listener = ans.getArgument(3);
            listener.onResponse(true);
            return null;
        }).when(persistentTasksService).waitForPersistentTasksCondition(any(), any(), any(), any());
        upgradeModeSuccessfullyChanged(new SetUpgradeModeActionRequest(false), stateWithTransformTask(), assertNoFailureListener(r -> {
            assertThat(r, is(AcknowledgedResponse.TRUE));
            verify(clusterService, never()).submitUnbatchedStateUpdateTask(eq("unassign persistent task from any node"), any());
        }));
    }
}
