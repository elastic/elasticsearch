/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.TransportVersionsFixupListener.NodeTransportVersionTask;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

public class TransportVersionsFixupListenerTests extends ESTestCase {

    // TODO: replace with real constants when 8.8.0 is released
    private static final Version NEXT_VERSION = Version.fromString("8.8.1");
    private static final TransportVersion NEXT_TRANSPORT_VERSION = TransportVersion.fromId(NEXT_VERSION.id);

    @SuppressWarnings("unchecked")
    private static MasterServiceTaskQueue<NodeTransportVersionTask> newMockTaskQueue() {
        return mock(MasterServiceTaskQueue.class);
    }

    private static DiscoveryNodes node(Version... versions) {
        var builder = DiscoveryNodes.builder();
        for (int i = 0; i < versions.length; i++) {
            builder.add(DiscoveryNodeUtils.create("node" + i, new TransportAddress(TransportAddress.META_ADDRESS, 9200 + i), versions[i]));
        }
        builder.localNodeId("node0").masterNodeId("node0");
        return builder.build();
    }

    @SafeVarargs
    private static <T> Map<String, T> versions(T... versions) {
        Map<String, T> tvs = new HashMap<>();
        for (int i = 0; i < versions.length; i++) {
            tvs.put("node" + i, versions[i]);
        }
        return tvs;
    }

    private static NodesInfoResponse getResponse(Map<String, TransportVersion> responseData) {
        return new NodesInfoResponse(
            ClusterName.DEFAULT,
            responseData.entrySet()
                .stream()
                .map(
                    e -> new NodeInfo(
                        null,
                        e.getValue(),
                        null,
                        DiscoveryNodeUtils.create(e.getKey(), new TransportAddress(TransportAddress.META_ADDRESS, 9200)),
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null
                    )
                )
                .toList(),
            List.of()
        );
    }

    public void testNothingFixedWhenNothingToInfer() {
        MasterServiceTaskQueue<NodeTransportVersionTask> taskQueue = newMockTaskQueue();
        ClusterAdminClient client = mock(ClusterAdminClient.class);

        ClusterState testState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(node(Version.V_8_8_0))
            .transportVersions(versions(TransportVersion.V_8_8_0))
            .build();

        TransportVersionsFixupListener listeners = new TransportVersionsFixupListener(taskQueue, client, null);
        listeners.clusterChanged(new ClusterChangedEvent("test", testState, ClusterState.EMPTY_STATE));

        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testNothingFixedWhenOnNextVersion() {
        MasterServiceTaskQueue<NodeTransportVersionTask> taskQueue = newMockTaskQueue();
        ClusterAdminClient client = mock(ClusterAdminClient.class);

        ClusterState testState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(node(NEXT_VERSION))
            .transportVersions(versions(NEXT_TRANSPORT_VERSION))
            .build();

        TransportVersionsFixupListener listeners = new TransportVersionsFixupListener(taskQueue, client, null);
        listeners.clusterChanged(new ClusterChangedEvent("test", testState, ClusterState.EMPTY_STATE));

        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testNothingFixedWhenOnPreviousVersion() {
        MasterServiceTaskQueue<NodeTransportVersionTask> taskQueue = newMockTaskQueue();
        ClusterAdminClient client = mock(ClusterAdminClient.class);

        ClusterState testState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(node(Version.V_8_7_0, Version.V_8_8_0))
            .transportVersions(versions(TransportVersion.V_8_7_0, TransportVersion.V_8_8_0))
            .build();

        TransportVersionsFixupListener listeners = new TransportVersionsFixupListener(taskQueue, client, null);
        listeners.clusterChanged(new ClusterChangedEvent("test", testState, ClusterState.EMPTY_STATE));

        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testVersionsAreFixed() {
        MasterServiceTaskQueue<NodeTransportVersionTask> taskQueue = newMockTaskQueue();
        ClusterAdminClient client = mock(ClusterAdminClient.class);

        ClusterState testState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(node(NEXT_VERSION, NEXT_VERSION, NEXT_VERSION))
            .transportVersions(versions(NEXT_TRANSPORT_VERSION, TransportVersion.V_8_8_0, TransportVersion.V_8_8_0))
            .build();

        ArgumentCaptor<ActionListener<NodesInfoResponse>> action = ArgumentCaptor.forClass(ActionListener.class);
        ArgumentCaptor<NodeTransportVersionTask> task = ArgumentCaptor.forClass(NodeTransportVersionTask.class);

        TransportVersionsFixupListener listeners = new TransportVersionsFixupListener(taskQueue, client, null);
        listeners.clusterChanged(new ClusterChangedEvent("test", testState, ClusterState.EMPTY_STATE));
        verify(client).nodesInfo(
            argThat(transformedMatch(NodesInfoRequest::nodesIds, arrayContainingInAnyOrder("node1", "node2"))),
            action.capture()
        );
        action.getValue().onResponse(getResponse(Map.of("node1", NEXT_TRANSPORT_VERSION, "node2", NEXT_TRANSPORT_VERSION)));
        verify(taskQueue).submitTask(anyString(), task.capture(), any());

        assertThat(task.getValue().results(), equalTo(Map.of("node1", NEXT_TRANSPORT_VERSION, "node2", NEXT_TRANSPORT_VERSION)));
    }

    public void testConcurrentChangesDoNotOverlap() {
        MasterServiceTaskQueue<NodeTransportVersionTask> taskQueue = newMockTaskQueue();
        ClusterAdminClient client = mock(ClusterAdminClient.class);

        ClusterState testState1 = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(node(NEXT_VERSION, NEXT_VERSION, NEXT_VERSION))
            .transportVersions(versions(NEXT_TRANSPORT_VERSION, TransportVersion.V_8_8_0, TransportVersion.V_8_8_0))
            .build();

        TransportVersionsFixupListener listeners = new TransportVersionsFixupListener(taskQueue, client, null);
        listeners.clusterChanged(new ClusterChangedEvent("test", testState1, ClusterState.EMPTY_STATE));
        verify(client).nodesInfo(argThat(transformedMatch(NodesInfoRequest::nodesIds, arrayContainingInAnyOrder("node1", "node2"))), any());
        // don't send back the response yet

        ClusterState testState2 = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(node(NEXT_VERSION, NEXT_VERSION, NEXT_VERSION))
            .transportVersions(versions(NEXT_TRANSPORT_VERSION, NEXT_TRANSPORT_VERSION, TransportVersion.V_8_8_0))
            .build();
        // should not send any requests
        listeners.clusterChanged(new ClusterChangedEvent("test", testState2, testState1));
        verifyNoMoreInteractions(client);
    }

    @SuppressWarnings("unchecked")
    public void testFailedRequestsAreRetried() {
        MasterServiceTaskQueue<NodeTransportVersionTask> taskQueue = newMockTaskQueue();
        ClusterAdminClient client = mock(ClusterAdminClient.class);
        Scheduler scheduler = mock(Scheduler.class);

        ClusterState testState1 = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(node(NEXT_VERSION, NEXT_VERSION, NEXT_VERSION))
            .transportVersions(versions(NEXT_TRANSPORT_VERSION, TransportVersion.V_8_8_0, TransportVersion.V_8_8_0))
            .build();

        ArgumentCaptor<ActionListener<NodesInfoResponse>> action = ArgumentCaptor.forClass(ActionListener.class);
        ArgumentCaptor<Runnable> retry = ArgumentCaptor.forClass(Runnable.class);

        TransportVersionsFixupListener listeners = new TransportVersionsFixupListener(taskQueue, client, scheduler);
        listeners.clusterChanged(new ClusterChangedEvent("test", testState1, ClusterState.EMPTY_STATE));
        verify(client, times(1)).nodesInfo(any(), action.capture());
        // do response immediately
        action.getValue().onFailure(new RuntimeException("failure"));
        verify(scheduler).schedule(retry.capture(), any(), any());

        // running retry should cause another check
        retry.getValue().run();
        verify(client, times(2)).nodesInfo(
            argThat(transformedMatch(NodesInfoRequest::nodesIds, arrayContainingInAnyOrder("node1", "node2"))),
            any()
        );
    }
}
