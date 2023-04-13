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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.TransportVersionsFixupListener.NodeTransportVersionTask;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchMatchers;
import org.hamcrest.Matcher;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TransportVersionsFixupListenerTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    private static MasterServiceTaskQueue<NodeTransportVersionTask> newMockTaskQueue() {
        return mock(MasterServiceTaskQueue.class);
    }

    private static DiscoveryNodes node(Version... versions) {
        var builder = DiscoveryNodes.builder();
        for (int i = 0; i < versions.length; i++) {
            builder.add(new DiscoveryNode("node" + i, new TransportAddress(TransportAddress.META_ADDRESS, i), versions[i]));
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
                        new DiscoveryNode(e.getKey(), new TransportAddress(TransportAddress.META_ADDRESS, 1), null),
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

    @SuppressWarnings({ "rawtypes" })
    private static ActionListener[] captureListener(ClusterAdminClient client, Matcher<NodesInfoRequest> request) {
        ActionListener[] listener = new ActionListener[1];
        doAnswer(i -> {
            assertThat(i.getArgument(0), request);
            listener[0] = i.getArgument(1);
            return null;
        }).when(client).nodesInfo(any(), any());
        return listener;
    }

    private static NodeTransportVersionTask[] captureTask(MasterServiceTaskQueue<NodeTransportVersionTask> taskQueue) {
        NodeTransportVersionTask[] task = new NodeTransportVersionTask[1];
        doAnswer(i -> {
            task[0] = i.getArgument(1);
            return null;
        }).when(taskQueue).submitTask(anyString(), any(), any());
        return task;
    }

    public void testNothingFixedWhenNothingToInfer() {
        MasterServiceTaskQueue<NodeTransportVersionTask> taskQueue = newMockTaskQueue();
        ClusterAdminClient client = mock(ClusterAdminClient.class);

        ClusterState testState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(node(Version.V_8_8_0, Version.V_8_8_0))
            .transportVersions(versions(TransportVersion.V_8_8_0, TransportVersion.V_8_8_0))
            .build();

        TransportVersionsFixupListener listeners = new TransportVersionsFixupListener(taskQueue, client);
        listeners.clusterChanged(new ClusterChangedEvent("test", testState, ClusterState.EMPTY_STATE));

        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testNothingFixedWhenOnPreviousVersion() {
        MasterServiceTaskQueue<NodeTransportVersionTask> taskQueue = newMockTaskQueue();
        ClusterAdminClient client = mock(ClusterAdminClient.class);

        ClusterState testState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(node(Version.V_8_7_0, Version.V_8_8_0))
            .transportVersions(versions(TransportVersion.V_8_8_0, TransportVersion.V_8_8_0))
            .build();

        TransportVersionsFixupListener listeners = new TransportVersionsFixupListener(taskQueue, client);
        listeners.clusterChanged(new ClusterChangedEvent("test", testState, ClusterState.EMPTY_STATE));

        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testVersionsAreFixed() {
        MasterServiceTaskQueue<NodeTransportVersionTask> taskQueue = newMockTaskQueue();
        ClusterAdminClient client = mock(ClusterAdminClient.class);

        ClusterState testState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(node(Version.V_8_8_0, Version.V_8_8_0, Version.V_8_8_0))
            .actualTransportVersions(versions(Optional.of(TransportVersion.V_8_8_0), Optional.empty(), Optional.empty()))
            .build();

        var action = captureListener(
            client,
            ElasticsearchMatchers.HasPropertyLambdaMatcher.hasProperty(
                NodesInfoRequest::nodesIds,
                arrayContainingInAnyOrder("node1", "node2")
            )
        );
        NodeTransportVersionTask[] task = captureTask(taskQueue);

        TransportVersionsFixupListener listeners = new TransportVersionsFixupListener(taskQueue, client);
        listeners.clusterChanged(new ClusterChangedEvent("test", testState, ClusterState.EMPTY_STATE));
        action[0].onResponse(getResponse(Map.of("node1", TransportVersion.V_8_8_0, "node2", TransportVersion.V_8_8_0)));

        assertThat(task[0].results(), equalTo(Map.of("node1", TransportVersion.V_8_8_0, "node2", TransportVersion.V_8_8_0)));
    }

    public void testConcurrentChangesDoNotOverlap() {
        MasterServiceTaskQueue<NodeTransportVersionTask> taskQueue = newMockTaskQueue();
        ClusterAdminClient client = mock(ClusterAdminClient.class);

        ClusterState testState1 = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(node(Version.V_8_8_0, Version.V_8_8_0, Version.V_8_8_0))
            .actualTransportVersions(versions(Optional.of(TransportVersion.V_8_8_0), Optional.empty(), Optional.empty()))
            .build();

        var action1 = captureListener(
            client,
            ElasticsearchMatchers.HasPropertyLambdaMatcher.hasProperty(
                NodesInfoRequest::nodesIds,
                arrayContainingInAnyOrder("node1", "node2")
            )
        );

        TransportVersionsFixupListener listeners = new TransportVersionsFixupListener(taskQueue, client);
        listeners.clusterChanged(new ClusterChangedEvent("test", testState1, ClusterState.EMPTY_STATE));
        verify(client).nodesInfo(any(), any());
        // don't send back the response yet

        ClusterState testState2 = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(node(Version.V_8_8_0, Version.V_8_8_0, Version.V_8_8_0))
            .actualTransportVersions(
                versions(Optional.of(TransportVersion.V_8_8_0), Optional.of(TransportVersion.V_8_8_0), Optional.empty())
            )
            .build();
        // should not send any requests
        listeners.clusterChanged(new ClusterChangedEvent("test", testState2, testState1));
        verifyNoMoreInteractions(client);
    }
}
