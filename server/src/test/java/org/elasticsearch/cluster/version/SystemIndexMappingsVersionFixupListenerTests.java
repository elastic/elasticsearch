/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.version;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.version.SystemIndexMappingsVersions;
import org.elasticsearch.action.admin.cluster.version.SystemIndexMappingsVersionsRequest;
import org.elasticsearch.action.admin.cluster.version.SystemIndexMappingsVersionsResponse;
import org.elasticsearch.action.admin.cluster.version.TransportSystemIndexMappingsVersionsAction;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Map.entry;
import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
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

public class SystemIndexMappingsVersionFixupListenerTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    private static MasterServiceTaskQueue<SystemIndexMappingsVersionFixupListener.SystemIndexMappingsVersionTask> newMockTaskQueue() {
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

    private static SystemIndexMappingsVersionsResponse getResponse(Map<String, CompatibilityVersions> responseData) {
        return new SystemIndexMappingsVersionsResponse(
            ClusterName.DEFAULT,
            responseData.entrySet()
                .stream()
                .map(
                    e -> new SystemIndexMappingsVersions(
                        e.getValue().systemIndexMappingsVersion(),
                        DiscoveryNodeUtils.create(e.getKey(), new TransportAddress(TransportAddress.META_ADDRESS, 9200))
                    )
                )
                .toList(),
            List.of()
        );
    }

    public void testNothingDoneWhenNothingToFix() {
        MasterServiceTaskQueue<SystemIndexMappingsVersionFixupListener.SystemIndexMappingsVersionTask> taskQueue = newMockTaskQueue();
        ClusterAdminClient client = mock(ClusterAdminClient.class);

        var nodes = nodes(Version.CURRENT, Version.CURRENT);
        var versions = nodes.stream()
            .map(DiscoveryNode::getId)
            .collect(
                Collectors.toUnmodifiableMap(
                    Function.identity(),
                    x -> new CompatibilityVersions(
                        TransportVersion.current(),
                        Map.of(".system-index-1", new SystemIndexDescriptor.MappingsVersion(1, 1234))
                    )
                )
            );

        ClusterState testState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(nodes)
            .nodeIdsToCompatibilityVersions(versions)
            .build();

        var listener = new SystemIndexMappingsVersionFixupListener(taskQueue, client, null, null);
        listener.clusterChanged(new ClusterChangedEvent("test", testState, ClusterState.EMPTY_STATE));

        verify(taskQueue, never()).submitTask(anyString(), any(), any());
    }

    public void testFeaturesFixedAfterNewMaster() throws Exception {
        MasterServiceTaskQueue<SystemIndexMappingsVersionFixupListener.SystemIndexMappingsVersionTask> taskQueue = newMockTaskQueue();
        ClusterAdminClient client = mock(ClusterAdminClient.class);

        var compatibilityVersions = new CompatibilityVersions(
            TransportVersion.current(),
            Map.of(".system-index-1", new SystemIndexDescriptor.MappingsVersion(1, 1234))
        );

        var nodes = nodes(Version.CURRENT, Version.CURRENT, Version.CURRENT);
        ClusterState testState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(nodes)
            .nodeIdsToCompatibilityVersions(
                Map.ofEntries(
                    entry("node0", compatibilityVersions),
                    entry("node1", CompatibilityVersions.EMPTY),
                    entry("node2", CompatibilityVersions.EMPTY)
                )
            )
            .build();

        ArgumentCaptor<ActionListener<SystemIndexMappingsVersionsResponse>> action = ArgumentCaptor.captor();
        ArgumentCaptor<SystemIndexMappingsVersionFixupListener.SystemIndexMappingsVersionTask> task = ArgumentCaptor.captor();

        var listener = new SystemIndexMappingsVersionFixupListener(taskQueue, client, null, null);
        listener.clusterChanged(new ClusterChangedEvent("test", testState, ClusterState.EMPTY_STATE));
        verify(client).execute(
            eq(TransportSystemIndexMappingsVersionsAction.TYPE),
            argThat(transformedMatch(SystemIndexMappingsVersionsRequest::nodesIds, arrayContainingInAnyOrder("node1", "node2"))),
            action.capture()
        );

        action.getValue().onResponse(getResponse(Map.of("node1", compatibilityVersions, "node2", compatibilityVersions)));
        verify(taskQueue).submitTask(anyString(), task.capture(), any());

        ClusterState newState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            testState,
            new SystemIndexMappingsVersionFixupListener.SystemIndexMappingsVersionUpdater(),
            List.of(task.getValue())
        );

        assertThat(
            newState.compatibilityVersions().values(),
            everyItem(
                transformedMatch(
                    CompatibilityVersions::systemIndexMappingsVersion,
                    equalTo(compatibilityVersions.systemIndexMappingsVersion())
                )
            )
        );
    }

    public void testFeaturesFetchedOnlyForUpdatedNodes() {
        MasterServiceTaskQueue<SystemIndexMappingsVersionFixupListener.SystemIndexMappingsVersionTask> taskQueue = newMockTaskQueue();
        ClusterAdminClient client = mock(ClusterAdminClient.class);

        var compatibilityVersions = new CompatibilityVersions(
            TransportVersion.current(),
            Map.of(".system-index-1", new SystemIndexDescriptor.MappingsVersion(1, 1234))
        );
        ClusterState testState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(
                nodes(
                    VersionInformation.CURRENT,
                    VersionInformation.CURRENT,
                    new VersionInformation(Version.V_8_15_0, IndexVersion.current(), IndexVersion.current())
                )
            )
            .nodeIdsToCompatibilityVersions(
                Map.ofEntries(
                    entry("node0", compatibilityVersions),
                    entry("node1", CompatibilityVersions.EMPTY),
                    entry("node2", CompatibilityVersions.EMPTY)
                )
            )
            .build();

        ArgumentCaptor<ActionListener<SystemIndexMappingsVersionsResponse>> action = ArgumentCaptor.captor();

        var listener = new SystemIndexMappingsVersionFixupListener(taskQueue, client, null, null);
        listener.clusterChanged(new ClusterChangedEvent("test", testState, ClusterState.EMPTY_STATE));
        verify(client).execute(
            eq(TransportSystemIndexMappingsVersionsAction.TYPE),
            argThat(transformedMatch(SystemIndexMappingsVersionsRequest::nodesIds, arrayContainingInAnyOrder("node1"))),
            action.capture()
        );
    }

    public void testConcurrentChangesDoNotOverlap() {
        MasterServiceTaskQueue<SystemIndexMappingsVersionFixupListener.SystemIndexMappingsVersionTask> taskQueue = newMockTaskQueue();
        ClusterAdminClient client = mock(ClusterAdminClient.class);

        var compatibilityVersions = new CompatibilityVersions(
            TransportVersion.current(),
            Map.of(".system-index-1", new SystemIndexDescriptor.MappingsVersion(1, 1234))
        );
        ClusterState testState1 = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(nodes(Version.CURRENT, Version.CURRENT, Version.CURRENT))
            .nodeIdsToCompatibilityVersions(
                Map.ofEntries(
                    entry("node0", compatibilityVersions),
                    entry("node1", CompatibilityVersions.EMPTY),
                    entry("node2", CompatibilityVersions.EMPTY)
                )
            )
            .build();

        var listener = new SystemIndexMappingsVersionFixupListener(taskQueue, client, null, null);
        listener.clusterChanged(new ClusterChangedEvent("test", testState1, ClusterState.EMPTY_STATE));
        verify(client).execute(
            eq(TransportSystemIndexMappingsVersionsAction.TYPE),
            argThat(transformedMatch(SystemIndexMappingsVersionsRequest::nodesIds, arrayContainingInAnyOrder("node1", "node2"))),
            any()
        );
        // don't send back the response yet
        ClusterState testState2 = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(nodes(Version.CURRENT, Version.CURRENT, Version.CURRENT))
            .nodeIdsToCompatibilityVersions(
                Map.ofEntries(
                    entry("node0", compatibilityVersions),
                    entry("node1", compatibilityVersions),
                    entry("node2", CompatibilityVersions.EMPTY)
                )
            )
            .build();
        // should not send any requests
        listener.clusterChanged(new ClusterChangedEvent("test", testState2, testState1));
        verifyNoMoreInteractions(client);
    }

    public void testFailedRequestsAreRetried() {
        MasterServiceTaskQueue<SystemIndexMappingsVersionFixupListener.SystemIndexMappingsVersionTask> taskQueue = newMockTaskQueue();
        ClusterAdminClient client = mock(ClusterAdminClient.class);
        Scheduler scheduler = mock(Scheduler.class);
        Executor executor = mock(Executor.class);

        var compatibilityVersions = new CompatibilityVersions(
            TransportVersion.current(),
            Map.of(".system-index-1", new SystemIndexDescriptor.MappingsVersion(1, 1234))
        );
        ClusterState testState = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(nodes(Version.CURRENT, Version.CURRENT, Version.CURRENT))
            .nodeIdsToCompatibilityVersions(
                Map.ofEntries(
                    entry("node0", compatibilityVersions),
                    entry("node1", CompatibilityVersions.EMPTY),
                    entry("node2", CompatibilityVersions.EMPTY)
                )
            )
            .build();

        ArgumentCaptor<ActionListener<SystemIndexMappingsVersionsResponse>> action = ArgumentCaptor.captor();
        ArgumentCaptor<Runnable> retry = ArgumentCaptor.forClass(Runnable.class);

        var listener = new SystemIndexMappingsVersionFixupListener(taskQueue, client, scheduler, executor);
        listener.clusterChanged(new ClusterChangedEvent("test", testState, ClusterState.EMPTY_STATE));
        verify(client).execute(
            eq(TransportSystemIndexMappingsVersionsAction.TYPE),
            argThat(transformedMatch(SystemIndexMappingsVersionsRequest::nodesIds, arrayContainingInAnyOrder("node1", "node2"))),
            action.capture()
        );

        action.getValue().onFailure(new RuntimeException("failure"));
        verify(scheduler).schedule(retry.capture(), any(), same(executor));

        // running the retry should cause another call
        retry.getValue().run();
        verify(client, times(2)).execute(
            eq(TransportSystemIndexMappingsVersionsAction.TYPE),
            argThat(transformedMatch(SystemIndexMappingsVersionsRequest::nodesIds, arrayContainingInAnyOrder("node1", "node2"))),
            action.capture()
        );
    }
}
