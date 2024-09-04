/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.features;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.features.NodeFeatures;
import org.elasticsearch.action.admin.cluster.node.features.NodesFeaturesRequest;
import org.elasticsearch.action.admin.cluster.node.features.NodesFeaturesResponse;
import org.elasticsearch.action.admin.cluster.node.features.TransportNodesFeaturesAction;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.features.NodeFeaturesFixupListener.NodesFeaturesTask;
import org.elasticsearch.cluster.features.NodeFeaturesFixupListener.NodesFeaturesUpdater;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

public class NodeFeaturesFixupListenerTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    private static MasterServiceTaskQueue<NodesFeaturesTask> newMockTaskQueue() {
        return mock(MasterServiceTaskQueue.class);
    }

    private static DiscoveryNodes nodes(Version... versions) {
        var builder = DiscoveryNodes.builder();
        for (int i = 0; i < versions.length; i++) {
            builder.add(DiscoveryNodeUtils.create("node" + i, new TransportAddress(TransportAddress.META_ADDRESS, 9200 + i), versions[i]));
        }
        builder.localNodeId("node0").masterNodeId("node0");
        return builder.build();
    }

    private static DiscoveryNodes nodes(VersionInformation... versions) {
        var builder = DiscoveryNodes.builder();
        for (int i = 0; i < versions.length; i++) {
            builder.add(
                DiscoveryNodeUtils.builder("node" + i)
                    .address(new TransportAddress(TransportAddress.META_ADDRESS, 9200 + i))
                    .version(versions[i])
                    .build()
            );
        }
        builder.localNodeId("node0").masterNodeId("node0");
        return builder.build();
    }

    @SafeVarargs
    private static Map<String, Set<String>> features(Set<String>... nodeFeatures) {
        Map<String, Set<String>> features = new HashMap<>();
        for (int i = 0; i < nodeFeatures.length; i++) {
            features.put("node" + i, nodeFeatures[i]);
        }
        return features;
    }

    private static NodesFeaturesResponse getResponse(Map<String, Set<String>> responseData) {
        return new NodesFeaturesResponse(
            ClusterName.DEFAULT,
            responseData.entrySet()
                .stream()
                .map(
                    e -> new NodeFeatures(
                        e.getValue(),
                        DiscoveryNodeUtils.create(e.getKey(), new TransportAddress(TransportAddress.META_ADDRESS, 9200))
                    )
                )
                .toList(),
            List.of()
        );
    }

    public void testNothingDoneWhenNothingToFix() {
        MasterServiceTaskQueue<NodesFeaturesTask> taskQueue = newMockTaskQueue();
        ClusterAdminClient client = mock(ClusterAdminClient.class);

        ClusterState testState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(nodes(Version.CURRENT, Version.CURRENT))
            .nodeFeatures(features(Set.of("f1", "f2"), Set.of("f1", "f2")))
            .build();

        NodeFeaturesFixupListener listener = new NodeFeaturesFixupListener(taskQueue, client, null, null);
        listener.clusterChanged(new ClusterChangedEvent("test", testState, ClusterState.EMPTY_STATE));

        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testFeaturesFixedAfterNewMaster() throws Exception {
        MasterServiceTaskQueue<NodesFeaturesTask> taskQueue = newMockTaskQueue();
        ClusterAdminClient client = mock(ClusterAdminClient.class);
        Set<String> features = Set.of("f1", "f2");

        ClusterState testState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(nodes(Version.CURRENT, Version.CURRENT, Version.CURRENT))
            .nodeFeatures(features(features, Set.of(), Set.of()))
            .build();

        ArgumentCaptor<ActionListener<NodesFeaturesResponse>> action = ArgumentCaptor.captor();
        ArgumentCaptor<NodesFeaturesTask> task = ArgumentCaptor.captor();

        NodeFeaturesFixupListener listener = new NodeFeaturesFixupListener(taskQueue, client, null, null);
        listener.clusterChanged(new ClusterChangedEvent("test", testState, ClusterState.EMPTY_STATE));
        verify(client).execute(
            eq(TransportNodesFeaturesAction.TYPE),
            argThat(transformedMatch(NodesFeaturesRequest::nodesIds, arrayContainingInAnyOrder("node1", "node2"))),
            action.capture()
        );

        action.getValue().onResponse(getResponse(Map.of("node1", features, "node2", features)));
        verify(taskQueue).submitTask(anyString(), task.capture(), any());

        ClusterState newState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            testState,
            new NodesFeaturesUpdater(),
            List.of(task.getValue())
        );

        assertThat(newState.clusterFeatures().allNodeFeatures(), containsInAnyOrder("f1", "f2"));
    }

    public void testFeaturesFetchedOnlyForUpdatedNodes() {
        MasterServiceTaskQueue<NodesFeaturesTask> taskQueue = newMockTaskQueue();
        ClusterAdminClient client = mock(ClusterAdminClient.class);

        ClusterState testState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(
                nodes(
                    VersionInformation.CURRENT,
                    VersionInformation.CURRENT,
                    new VersionInformation(Version.V_8_12_0, IndexVersion.current(), IndexVersion.current())
                )
            )
            .nodeFeatures(features(Set.of("f1", "f2"), Set.of(), Set.of()))
            .build();

        ArgumentCaptor<ActionListener<NodesFeaturesResponse>> action = ArgumentCaptor.captor();

        NodeFeaturesFixupListener listener = new NodeFeaturesFixupListener(taskQueue, client, null, null);
        listener.clusterChanged(new ClusterChangedEvent("test", testState, ClusterState.EMPTY_STATE));
        verify(client).execute(
            eq(TransportNodesFeaturesAction.TYPE),
            argThat(transformedMatch(NodesFeaturesRequest::nodesIds, arrayContainingInAnyOrder("node1"))),
            action.capture()
        );
    }

    public void testConcurrentChangesDoNotOverlap() {
        MasterServiceTaskQueue<NodesFeaturesTask> taskQueue = newMockTaskQueue();
        ClusterAdminClient client = mock(ClusterAdminClient.class);
        Set<String> features = Set.of("f1", "f2");

        ClusterState testState1 = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(nodes(Version.CURRENT, Version.CURRENT, Version.CURRENT))
            .nodeFeatures(features(features, Set.of(), Set.of()))
            .build();

        NodeFeaturesFixupListener listeners = new NodeFeaturesFixupListener(taskQueue, client, null, null);
        listeners.clusterChanged(new ClusterChangedEvent("test", testState1, ClusterState.EMPTY_STATE));
        verify(client).execute(
            eq(TransportNodesFeaturesAction.TYPE),
            argThat(transformedMatch(NodesFeaturesRequest::nodesIds, arrayContainingInAnyOrder("node1", "node2"))),
            any()
        );
        // don't send back the response yet

        ClusterState testState2 = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(nodes(Version.CURRENT, Version.CURRENT, Version.CURRENT))
            .nodeFeatures(features(features, features, Set.of()))
            .build();
        // should not send any requests
        listeners.clusterChanged(new ClusterChangedEvent("test", testState2, testState1));
        verifyNoMoreInteractions(client);
    }

    public void testFailedRequestsAreRetried() {
        MasterServiceTaskQueue<NodesFeaturesTask> taskQueue = newMockTaskQueue();
        ClusterAdminClient client = mock(ClusterAdminClient.class);
        Scheduler scheduler = mock(Scheduler.class);
        Executor executor = mock(Executor.class);
        Set<String> features = Set.of("f1", "f2");

        ClusterState testState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(nodes(Version.CURRENT, Version.CURRENT, Version.CURRENT))
            .nodeFeatures(features(features, Set.of(), Set.of()))
            .build();

        ArgumentCaptor<ActionListener<NodesFeaturesResponse>> action = ArgumentCaptor.captor();
        ArgumentCaptor<Runnable> retry = ArgumentCaptor.forClass(Runnable.class);

        NodeFeaturesFixupListener listener = new NodeFeaturesFixupListener(taskQueue, client, scheduler, executor);
        listener.clusterChanged(new ClusterChangedEvent("test", testState, ClusterState.EMPTY_STATE));
        verify(client).execute(
            eq(TransportNodesFeaturesAction.TYPE),
            argThat(transformedMatch(NodesFeaturesRequest::nodesIds, arrayContainingInAnyOrder("node1", "node2"))),
            action.capture()
        );

        action.getValue().onFailure(new RuntimeException("failure"));
        verify(scheduler).schedule(retry.capture(), any(), same(executor));

        // running the retry should cause another call
        retry.getValue().run();
        verify(client, times(2)).execute(
            eq(TransportNodesFeaturesAction.TYPE),
            argThat(transformedMatch(NodesFeaturesRequest::nodesIds, arrayContainingInAnyOrder("node1", "node2"))),
            action.capture()
        );
    }
}
