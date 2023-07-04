/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ProfilingIndexManagerTests extends ESTestCase {
    private final AtomicBoolean templatesCreated = new AtomicBoolean();
    private ProfilingIndexManager indexManager;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private VerifyingClient client;

    @Before
    public void createRegistryAndClient() {
        templatesCreated.set(false);
        threadPool = new TestThreadPool(this.getClass().getName());
        client = new VerifyingClient(threadPool);
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        indexManager = new ProfilingIndexManager(threadPool, client, clusterService) {
            @Override
            protected boolean isAllResourcesCreated(ClusterChangedEvent event) {
                return templatesCreated.get();
            }
        };
        indexManager.setTemplatesEnabled(true);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testThatMissingMasterNodeDoesNothing() {
        DiscoveryNode localNode = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").add(localNode).build();

        client.setVerifier((a, r, l) -> {
            fail("if the master is missing nothing should happen");
            return null;
        });

        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyList(), nodes);
        indexManager.clusterChanged(event);
    }

    public void testThatMissingTemplatesDoesNothing() {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        client.setVerifier((a, r, l) -> {
            fail("if any templates are missing nothing should happen");
            return null;
        });

        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyList(), nodes);
        indexManager.clusterChanged(event);
    }

    public void testThatNonExistingIndicesAreAddedImmediately() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();
        templatesCreated.set(true);

        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyList(), nodes);

        AtomicInteger calledTimes = new AtomicInteger(0);

        client.setVerifier((action, request, listener) -> verifyIndexInstalled(calledTimes, action, request, listener));
        indexManager.clusterChanged(event);
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(ProfilingIndexManager.PROFILING_INDICES.size())));

        calledTimes.set(0);
    }

    public void testThatExistingIndicesAreNotCreatedTwice() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();
        templatesCreated.set(true);

        String existingIndex = randomFrom(ProfilingIndexManager.PROFILING_INDICES).toString();
        ClusterChangedEvent event = createClusterChangedEvent(List.of(existingIndex), nodes);

        AtomicInteger calledTimes = new AtomicInteger(0);

        client.setVerifier((action, request, listener) -> verifyIndexInstalled(calledTimes, action, request, listener));
        indexManager.clusterChanged(event);
        // should not create the existing index
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(ProfilingIndexManager.PROFILING_INDICES.size() - 1)));

        calledTimes.set(0);
    }

    private ActionResponse verifyIndexInstalled(
        AtomicInteger calledTimes,
        ActionType<?> action,
        ActionRequest request,
        ActionListener<?> listener
    ) {
        if (action instanceof CreateIndexAction) {
            calledTimes.incrementAndGet();
            assertThat(action, instanceOf(CreateIndexAction.class));
            assertThat(request, instanceOf(CreateIndexRequest.class));
            assertNotNull(listener);
            return new CreateIndexResponse(true, true, ((CreateIndexRequest) request).index());
        } else {
            fail("client called with unexpected request:" + request.toString());
            return null;
        }
    }

    private ClusterChangedEvent createClusterChangedEvent(Iterable<String> existingIndices, DiscoveryNodes nodes) {
        ClusterState cs = createClusterState(Settings.EMPTY, existingIndices, nodes);
        ClusterChangedEvent realEvent = new ClusterChangedEvent(
            "created-from-test",
            cs,
            ClusterState.builder(new ClusterName("test")).build()
        );
        ClusterChangedEvent event = spy(realEvent);
        when(event.localNodeMaster()).thenReturn(nodes.isLocalNodeElectedMaster());

        return event;
    }

    private ClusterState createClusterState(Settings nodeSettings, Iterable<String> existingIndices, DiscoveryNodes nodes) {
        Map<String, IndexMetadata> indices = new HashMap<>();
        for (String index : existingIndices) {
            IndexMetadata mockMetadata = mock(IndexMetadata.class);
            when(mockMetadata.getIndex()).thenReturn(new Index(index, index));
            when(mockMetadata.getCompatibilityVersion()).thenReturn(Version.CURRENT);
            indices.put(index, mockMetadata);
        }
        return ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().indices(indices).transientSettings(nodeSettings).build())
            .blocks(new ClusterBlocks.Builder().build())
            .nodes(nodes)
            .build();
    }
}
