/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.remote;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoMetrics;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.node.info.TransportNodesInfoAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.support.AbstractClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterServerInfo;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteClusterNodesActionTests extends ESTestCase {

    public void testDoExecuteForRemoteServerNodes() {
        final ThreadPool threadPool = mock(ThreadPool.class);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        // Prepare nodesInfo response
        final int numberOfNodes = randomIntBetween(1, 6);
        final List<NodeInfo> nodeInfos = new ArrayList<>();
        final Set<DiscoveryNode> expectedRemoteServerNodes = new HashSet<>();
        for (int i = 0; i < numberOfNodes; i++) {
            final DiscoveryNode node = randomNode(i);
            final boolean remoteServerEnabled = randomBoolean();
            final TransportAddress remoteClusterProfileAddress = buildNewFakeTransportAddress();
            final RemoteClusterServerInfo remoteClusterServerInfo;
            if (remoteServerEnabled) {
                expectedRemoteServerNodes.add(node.withTransportAddress(remoteClusterProfileAddress));
                remoteClusterServerInfo = new RemoteClusterServerInfo(
                    new BoundTransportAddress(new TransportAddress[] { remoteClusterProfileAddress }, remoteClusterProfileAddress)
                );
            } else {
                remoteClusterServerInfo = null;
            }
            nodeInfos.add(
                new NodeInfo(
                    Build.current().version(),
                    new CompatibilityVersions(TransportVersion.current(), Map.of()),
                    IndexVersion.current(),
                    Map.of(),
                    null,
                    node,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    remoteClusterServerInfo,
                    null,
                    null,
                    null,
                    null
                )
            );
        }

        final NodesInfoResponse nodesInfoResponse = new NodesInfoResponse(
            new ClusterName(randomAlphaOfLengthBetween(3, 8)),
            nodeInfos,
            List.of()
        );

        final RemoteClusterNodesAction.TransportAction action = new RemoteClusterNodesAction.TransportAction(
            mock(TransportService.class),
            new ActionFilters(Set.of()),
            new AbstractClient(Settings.EMPTY, threadPool, TestProjectResolvers.alwaysThrow()) {
                @SuppressWarnings("unchecked")
                @Override
                protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    assertThat(threadContext.isSystemContext(), is(true));
                    assertSame(TransportNodesInfoAction.TYPE, action);
                    assertThat(
                        asInstanceOf(NodesInfoRequest.class, request).requestedMetrics(),
                        containsInAnyOrder(NodesInfoMetrics.Metric.REMOTE_CLUSTER_SERVER.metricName())
                    );
                    listener.onResponse((Response) nodesInfoResponse);
                }
            }
        );

        final PlainActionFuture<RemoteClusterNodesAction.Response> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), RemoteClusterNodesAction.Request.REMOTE_CLUSTER_SERVER_NODES, ActionListener.wrap(response -> {
            assertThat(threadContext.isSystemContext(), is(false));
            future.onResponse(response);
        }, future::onFailure));

        final List<DiscoveryNode> actualNodes = future.actionGet().getNodes();
        assertThat(Set.copyOf(actualNodes), equalTo(expectedRemoteServerNodes));
        assertThat(
            actualNodes.stream().map(DiscoveryNode::getAddress).collect(Collectors.toUnmodifiableSet()),
            equalTo(expectedRemoteServerNodes.stream().map(DiscoveryNode::getAddress).collect(Collectors.toUnmodifiableSet()))
        );
    }

    public void testDoExecuteForRemoteNodes() {
        final ThreadPool threadPool = mock(ThreadPool.class);
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        // Prepare nodesInfo response
        final int numberOfNodes = randomIntBetween(1, 6);
        final List<NodeInfo> nodeInfos = new ArrayList<>();
        final Set<DiscoveryNode> expectedRemoteNodes = new HashSet<>();
        for (int i = 0; i < numberOfNodes; i++) {
            final DiscoveryNode node = randomNode(i);
            expectedRemoteNodes.add(node);
            nodeInfos.add(
                new NodeInfo(
                    Build.current().version(),
                    new CompatibilityVersions(TransportVersion.current(), Map.of()),
                    IndexVersion.current(),
                    Map.of(),
                    null,
                    node,
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
            );
        }

        final NodesInfoResponse nodesInfoResponse = new NodesInfoResponse(
            new ClusterName(randomAlphaOfLengthBetween(3, 8)),
            nodeInfos,
            List.of()
        );

        final RemoteClusterNodesAction.TransportAction action = new RemoteClusterNodesAction.TransportAction(
            mock(TransportService.class),
            new ActionFilters(Set.of()),
            new AbstractClient(Settings.EMPTY, threadPool, TestProjectResolvers.alwaysThrow()) {
                @SuppressWarnings("unchecked")
                @Override
                protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    assertThat(threadContext.isSystemContext(), is(true));
                    assertSame(TransportNodesInfoAction.TYPE, action);
                    assertThat(asInstanceOf(NodesInfoRequest.class, request).requestedMetrics(), empty());
                    listener.onResponse((Response) nodesInfoResponse);
                }
            }
        );

        final PlainActionFuture<RemoteClusterNodesAction.Response> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), RemoteClusterNodesAction.Request.ALL_NODES, ActionListener.wrap(response -> {
            assertThat(threadContext.isSystemContext(), is(false));
            future.onResponse(response);
        }, future::onFailure));

        final List<DiscoveryNode> actualNodes = future.actionGet().getNodes();
        assertThat(Set.copyOf(actualNodes), equalTo(expectedRemoteNodes));
        assertThat(
            actualNodes.stream().map(DiscoveryNode::getAddress).collect(Collectors.toUnmodifiableSet()),
            equalTo(expectedRemoteNodes.stream().map(DiscoveryNode::getAddress).collect(Collectors.toUnmodifiableSet()))
        );
    }

    private DiscoveryNode randomNode(final int id) {
        return DiscoveryNodeUtils.builder(Integer.toString(id)).name("node-" + id).roles(Set.of()).build();
    }

}
