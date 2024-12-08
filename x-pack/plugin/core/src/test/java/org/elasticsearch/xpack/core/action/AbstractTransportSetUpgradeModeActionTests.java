/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractTransportSetUpgradeModeActionTests extends ESTestCase {
    /**
     * Creates a TaskQueue that invokes the SimpleBatchedExecutor.
     */
    public static ClusterService clusterService() {
        AtomicReference<SimpleBatchedExecutor<? extends ClusterStateTaskListener, Void>> executor = new AtomicReference<>();
        MasterServiceTaskQueue<ClusterStateTaskListener> taskQueue = mock();
        ClusterService clusterService = mock();
        doAnswer(ans -> {
            executor.set(ans.getArgument(2));
            return taskQueue;
        }).when(clusterService).createTaskQueue(any(), any(), any());
        doAnswer(ans -> {
            if (executor.get() == null) {
                fail("We should create the task queue before we submit tasks to it");
            } else {
                executor.get().executeTask(ans.getArgument(1), ClusterState.EMPTY_STATE);
                executor.get().taskSucceeded(ans.getArgument(1), null);
            }
            return null;
        }).when(taskQueue).submitTask(any(), any(), any());
        return clusterService;
    }

    /**
     * Creates a TaskQueue that calls the listener with an error.
     */
    public static ClusterService clusterServiceWithError(Exception e) {
        MasterServiceTaskQueue<ClusterStateTaskListener> taskQueue = mock();
        ClusterService clusterService = mock();
        when(clusterService.createTaskQueue(any(), any(), any())).thenReturn(taskQueue);
        doAnswer(ans -> {
            ClusterStateTaskListener listener = ans.getArgument(1);
            listener.onFailure(e);
            return null;
        }).when(taskQueue).submitTask(any(), any(), any());
        return clusterService;
    }

    /**
     * TaskQueue that does nothing.
     */
    public static ClusterService clusterServiceThatDoesNothing() {
        ClusterService clusterService = mock();
        when(clusterService.createTaskQueue(any(), any(), any())).thenReturn(mock());
        return clusterService;
    }

    public void testIdempotent() throws Exception {
        // create with update mode set to false
        var action = new TestTransportSetUpgradeModeAction(clusterServiceThatDoesNothing(), false);

        // flip to true but do nothing (cluster service mock won't invoke the listener)
        action.runWithoutWaiting(true);
        // call again
        var response = action.run(true);

        assertThat(response.v1(), nullValue());
        assertThat(response.v2(), notNullValue());
        assertThat(response.v2(), instanceOf(ElasticsearchStatusException.class));
        assertThat(
            response.v2().getMessage(),
            is("Cannot change [upgrade_mode] for feature name [" + action.featureName() + "]. Previous request is still being processed.")
        );
    }

    public void testUpdateDoesNotRun() throws Exception {
        var shouldNotChange = new AtomicBoolean(true);
        var action = new TestTransportSetUpgradeModeAction(true, l -> shouldNotChange.set(false));

        var response = action.run(true);

        assertThat(response.v1(), is(AcknowledgedResponse.TRUE));
        assertThat(response.v2(), nullValue());
        assertThat(shouldNotChange.get(), is(true));
    }

    public void testErrorReleasesLock() throws Exception {
        var action = new TestTransportSetUpgradeModeAction(false, l -> l.onFailure(new IllegalStateException("hello there")));

        action.run(true);
        var response = action.run(true);
        assertThat(
            "Previous request should have finished processing.",
            response.v2().getMessage(),
            not(containsString("Previous request is still being processed"))
        );
    }

    public void testErrorFromAction() throws Exception {
        var expectedException = new IllegalStateException("hello there");
        var action = new TestTransportSetUpgradeModeAction(false, l -> l.onFailure(expectedException));

        var response = action.run(true);

        assertThat(response.v1(), nullValue());
        assertThat(response.v2(), is(expectedException));
    }

    public void testErrorFromTaskQueue() throws Exception {
        var expectedException = new IllegalStateException("hello there");
        var action = new TestTransportSetUpgradeModeAction(clusterServiceWithError(expectedException), false);

        var response = action.run(true);

        assertThat(response.v1(), nullValue());
        assertThat(response.v2(), is(expectedException));
    }

    public void testSuccess() throws Exception {
        var action = new TestTransportSetUpgradeModeAction(false, l -> l.onResponse(AcknowledgedResponse.TRUE));

        var response = action.run(true);

        assertThat(response.v1(), is(AcknowledgedResponse.TRUE));
        assertThat(response.v2(), nullValue());
    }

    private static class TestTransportSetUpgradeModeAction extends AbstractTransportSetUpgradeModeAction {
        private final boolean upgradeMode;
        private final ClusterState updatedClusterState;
        private final Consumer<ActionListener<AcknowledgedResponse>> successFunc;

        TestTransportSetUpgradeModeAction(boolean upgradeMode, Consumer<ActionListener<AcknowledgedResponse>> successFunc) {
            super("actionName", "taskQueuePrefix", mock(), clusterService(), mock(), mock(), mock());
            this.upgradeMode = upgradeMode;
            this.updatedClusterState = ClusterState.EMPTY_STATE;
            this.successFunc = successFunc;
        }

        TestTransportSetUpgradeModeAction(ClusterService clusterService, boolean upgradeMode) {
            super("actionName", "taskQueuePrefix", mock(), clusterService, mock(), mock(), mock());
            this.upgradeMode = upgradeMode;
            this.updatedClusterState = ClusterState.EMPTY_STATE;
            this.successFunc = listener -> {};
        }

        public void runWithoutWaiting(boolean upgrade) throws Exception {
            masterOperation(mock(), new SetUpgradeModeActionRequest(upgrade), ClusterState.EMPTY_STATE, ActionListener.noop());
        }

        public Tuple<AcknowledgedResponse, Exception> run(boolean upgrade) throws Exception {
            AtomicReference<Tuple<AcknowledgedResponse, Exception>> response = new AtomicReference<>();
            CountDownLatch latch = new CountDownLatch(1);
            masterOperation(mock(), new SetUpgradeModeActionRequest(upgrade), ClusterState.EMPTY_STATE, ActionListener.wrap(r -> {
                response.set(Tuple.tuple(r, null));
                latch.countDown();
            }, e -> {
                response.set(Tuple.tuple(null, e));
                latch.countDown();
            }));
            assertTrue("Failed to run TestTransportSetUpgradeModeAction in 10s", latch.await(10, TimeUnit.SECONDS));
            return response.get();
        }

        @Override
        protected String featureName() {
            return "test-feature-name";
        }

        @Override
        protected boolean upgradeMode(ClusterState state) {
            return upgradeMode;
        }

        @Override
        protected ClusterState createUpdatedState(SetUpgradeModeActionRequest request, ClusterState state) {
            return updatedClusterState;
        }

        @Override
        protected void upgradeModeSuccessfullyChanged(
            Task task,
            SetUpgradeModeActionRequest request,
            ClusterState state,
            ActionListener<AcknowledgedResponse> listener
        ) {
            successFunc.accept(listener);
        }
    }
}
