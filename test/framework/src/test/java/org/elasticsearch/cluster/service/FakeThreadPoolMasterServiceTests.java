/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FakeThreadPoolMasterServiceTests extends ESTestCase {

    public void testFakeMasterService() {
        List<Runnable> runnableTasks = new ArrayList<>();
        AtomicReference<ClusterState> lastClusterStateRef = new AtomicReference<>();
        DiscoveryNode discoveryNode = DiscoveryNodeUtils.builder("node").roles(new HashSet<>(DiscoveryNodeRole.roles())).build();
        lastClusterStateRef.set(ClusterStateCreationUtils.state(discoveryNode, discoveryNode));
        long firstClusterStateVersion = lastClusterStateRef.get().version();
        AtomicReference<ActionListener<Void>> publishingCallback = new AtomicReference<>();
        final ThreadContext context = new ThreadContext(Settings.EMPTY);
        final ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.getThreadContext()).thenReturn(context);

        final ExecutorService executorService = mock(ExecutorService.class);
        doAnswer(invocationOnMock -> runnableTasks.add((Runnable) invocationOnMock.getArguments()[0])).when(executorService).execute(any());
        when(mockThreadPool.generic()).thenReturn(executorService);

        MasterService masterService = new FakeThreadPoolMasterService("test_node", mockThreadPool, runnableTasks::add);
        masterService.setClusterStateSupplier(lastClusterStateRef::get);
        masterService.setClusterStatePublisher((clusterStatePublicationEvent, publishListener, ackListener) -> {
            ClusterServiceUtils.setAllElapsedMillis(clusterStatePublicationEvent);
            lastClusterStateRef.set(clusterStatePublicationEvent.getNewState());
            publishingCallback.set(publishListener);
        });
        masterService.start();

        AtomicBoolean firstTaskCompleted = new AtomicBoolean();
        masterService.submitUnbatchedStateUpdateTask("test1", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return ClusterState.builder(currentState)
                    .metadata(Metadata.builder(currentState.metadata()).put(indexBuilder("test1")))
                    .build();
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                assertFalse(firstTaskCompleted.get());
                firstTaskCompleted.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError();
            }
        });
        assertThat(runnableTasks.size(), equalTo(1));
        assertThat(lastClusterStateRef.get().metadata().getProject().indices().size(), equalTo(0));
        assertThat(lastClusterStateRef.get().version(), equalTo(firstClusterStateVersion));
        assertNull(publishingCallback.get());
        assertFalse(firstTaskCompleted.get());

        final Runnable scheduleTask = runnableTasks.remove(0);
        assertThat(scheduleTask, hasToString("master service queue processor"));
        scheduleTask.run();

        // run tasks for computing routing nodes and indices lookup
        runnableTasks.remove(0).run();
        runnableTasks.remove(0).run();

        final Runnable publishTask = runnableTasks.remove(0);
        assertThat(publishTask, hasToString(containsString("publish change of cluster state")));
        publishTask.run();

        assertThat(lastClusterStateRef.get().metadata().getProject().indices().size(), equalTo(1));
        assertThat(lastClusterStateRef.get().version(), equalTo(firstClusterStateVersion + 1));
        assertNotNull(publishingCallback.get());
        assertFalse(firstTaskCompleted.get());
        assertThat(runnableTasks.size(), equalTo(0));

        AtomicBoolean secondTaskCompleted = new AtomicBoolean();
        masterService.submitUnbatchedStateUpdateTask("test2", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return ClusterState.builder(currentState)
                    .metadata(Metadata.builder(currentState.metadata()).put(indexBuilder("test2")))
                    .build();
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                assertFalse(secondTaskCompleted.get());
                secondTaskCompleted.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError();
            }
        });
        assertThat(runnableTasks.size(), equalTo(0));

        publishingCallback.getAndSet(null).onResponse(null);
        runnableTasks.remove(0).run(); // complete publication back on master service thread
        assertTrue(firstTaskCompleted.get());
        assertThat(runnableTasks.size(), equalTo(1)); // check that new task gets queued

        runnableTasks.remove(0).run(); // schedule again

        // run task for computing missing indices lookup
        runnableTasks.remove(0).run();

        runnableTasks.remove(0).run(); // publish again
        assertThat(lastClusterStateRef.get().metadata().getProject().indices().size(), equalTo(2));
        assertThat(lastClusterStateRef.get().version(), equalTo(firstClusterStateVersion + 2));
        assertNotNull(publishingCallback.get());
        assertFalse(secondTaskCompleted.get());
        publishingCallback.getAndSet(null).onResponse(null);
        runnableTasks.remove(0).run(); // complete publication back on master service thread
        assertTrue(secondTaskCompleted.get());
        assertThat(runnableTasks.size(), equalTo(0)); // check that no more tasks are queued
    }

    private static IndexMetadata.Builder indexBuilder(String index) {
        return IndexMetadata.builder(index).settings(indexSettings(IndexVersion.current(), 1, 0));
    }
}
