/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ProfilingDataStreamManagerTests extends ESTestCase {
    private final AtomicBoolean templatesCreated = new AtomicBoolean();
    private ProfilingDataStreamManager datastreamManager;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private VerifyingClient client;

    @Before
    public void createRegistryAndClient() {
        templatesCreated.set(false);
        threadPool = new TestThreadPool(this.getClass().getName());
        client = new VerifyingClient(threadPool);
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        datastreamManager = new ProfilingDataStreamManager(threadPool, client, clusterService) {
            @Override
            protected boolean isAllResourcesCreated(ClusterChangedEvent event) {
                return templatesCreated.get();
            }
        };
        datastreamManager.setTemplatesEnabled(true);
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
        datastreamManager.clusterChanged(event);
    }

    public void testThatMissingTemplatesDoesNothing() {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();

        client.setVerifier((a, r, l) -> {
            fail("if any templates are missing nothing should happen");
            return null;
        });

        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyList(), nodes);
        datastreamManager.clusterChanged(event);
    }

    public void testThatNonExistingDataStreamsAreAddedImmediately() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();
        templatesCreated.set(true);

        ClusterChangedEvent event = createClusterChangedEvent(Collections.emptyList(), nodes);

        AtomicInteger calledTimes = new AtomicInteger(0);

        client.setVerifier((action, request, listener) -> verifyDataStreamInstalled(calledTimes, action, request, listener));
        datastreamManager.clusterChanged(event);
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(ProfilingDataStreamManager.PROFILING_DATASTREAMS.size())));

        calledTimes.set(0);
    }

    public void testThatRedIndexIsNotTouched() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();
        templatesCreated.set(true);

        // This data stream is a rollover candidate
        ProfilingDataStreamManager.ProfilingDataStream existingDataStream = randomFrom(ProfilingDataStreamManager.PROFILING_DATASTREAMS);
        ClusterChangedEvent event = createClusterChangedEvent(
            List.of(existingDataStream.withVersion(0)),
            nodes,
            IndexMetadata.State.OPEN,
            false
        );

        AtomicInteger calledTimes = new AtomicInteger(0);

        client.setVerifier((action, request, listener) -> verifyDataStreamInstalled(calledTimes, action, request, listener));
        datastreamManager.clusterChanged(event);
        // should not create the index because a newer generation with the correct version exists
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(ProfilingDataStreamManager.PROFILING_DATASTREAMS.size() - 1)));

        calledTimes.set(0);
    }

    public void testThatClosedIndexIsNotTouched() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();
        templatesCreated.set(true);

        // This index is an upgrade candidate
        ProfilingDataStreamManager.ProfilingDataStream existingDataStream = randomFrom(ProfilingDataStreamManager.PROFILING_DATASTREAMS);
        ClusterChangedEvent event = createClusterChangedEvent(
            List.of(existingDataStream.withVersion(0)),
            nodes,
            IndexMetadata.State.CLOSE,
            true
        );

        AtomicInteger calledTimes = new AtomicInteger(0);

        client.setVerifier((action, request, listener) -> verifyDataStreamInstalled(calledTimes, action, request, listener));
        datastreamManager.clusterChanged(event);
        // should not create the index because a newer generation with the correct version exists
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(ProfilingDataStreamManager.PROFILING_DATASTREAMS.size() - 1)));

        calledTimes.set(0);
    }

    public void testThatExistingIndicesAreNotCreatedTwice() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();
        templatesCreated.set(true);

        ProfilingDataStreamManager.ProfilingDataStream existingDataStream = randomFrom(ProfilingDataStreamManager.PROFILING_DATASTREAMS);
        ClusterChangedEvent event = createClusterChangedEvent(List.of(existingDataStream), nodes);

        AtomicInteger calledTimes = new AtomicInteger(0);

        client.setVerifier((action, request, listener) -> verifyDataStreamInstalled(calledTimes, action, request, listener));
        datastreamManager.clusterChanged(event);
        // should not create the existing index
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(ProfilingDataStreamManager.PROFILING_DATASTREAMS.size() - 1)));

        calledTimes.set(0);
    }

    public void testThatDataStreamIsRolledOver() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();
        templatesCreated.set(true);

        ProfilingDataStreamManager.ProfilingDataStream dataStreamToRollover = randomFrom(ProfilingDataStreamManager.PROFILING_DATASTREAMS);
        List<ProfilingDataStreamManager.ProfilingDataStream> existingDataStreams = new ArrayList<>(
            ProfilingDataStreamManager.PROFILING_DATASTREAMS
        );
        existingDataStreams.remove(dataStreamToRollover);
        existingDataStreams.add(dataStreamToRollover.withVersion(0));

        ClusterChangedEvent event = createClusterChangedEvent(existingDataStreams, nodes);

        AtomicInteger calledTimes = new AtomicInteger(0);

        client.setVerifier((action, request, listener) -> verifyDataStreamRolledOver(calledTimes, action, request, listener));
        datastreamManager.clusterChanged(event);
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(1)));

        calledTimes.set(0);
    }

    private ActionResponse verifyDataStreamInstalled(
        AtomicInteger calledTimes,
        ActionType<?> action,
        ActionRequest request,
        ActionListener<?> listener
    ) {
        if (action instanceof CreateDataStreamAction) {
            calledTimes.incrementAndGet();
            assertThat(action, instanceOf(CreateDataStreamAction.class));
            assertThat(request, instanceOf(CreateDataStreamAction.Request.class));
            assertNotNull(listener);
            return AcknowledgedResponse.TRUE;
        } else {
            fail("client called with unexpected request:" + request.toString());
            return null;
        }
    }

    private ActionResponse verifyDataStreamRolledOver(
        AtomicInteger calledTimes,
        ActionType<?> action,
        ActionRequest request,
        ActionListener<?> listener
    ) {
        if (action instanceof RolloverAction) {
            calledTimes.incrementAndGet();
            assertThat(action, instanceOf(RolloverAction.class));
            assertThat(request, instanceOf(RolloverRequest.class));
            assertNotNull(listener);
            return AcknowledgedResponse.TRUE;
        } else {
            fail("client called with unexpected request:" + request.toString());
            return null;
        }
    }

    private ClusterChangedEvent createClusterChangedEvent(
        Iterable<ProfilingDataStreamManager.ProfilingDataStream> existingDataStreams,
        DiscoveryNodes nodes
    ) {
        return createClusterChangedEvent(existingDataStreams, nodes, IndexMetadata.State.OPEN, true);
    }

    private ClusterChangedEvent createClusterChangedEvent(
        Iterable<ProfilingDataStreamManager.ProfilingDataStream> existingDataStreams,
        DiscoveryNodes nodes,
        IndexMetadata.State state,
        boolean allShardsAssigned
    ) {
        ClusterState cs = createClusterState(Settings.EMPTY, existingDataStreams, nodes, state, allShardsAssigned);
        ClusterChangedEvent realEvent = new ClusterChangedEvent(
            "created-from-test",
            cs,
            ClusterState.builder(new ClusterName("test")).build()
        );
        ClusterChangedEvent event = spy(realEvent);
        when(event.localNodeMaster()).thenReturn(nodes.isLocalNodeElectedMaster());

        return event;
    }

    private ClusterState createClusterState(
        Settings nodeSettings,
        Iterable<ProfilingDataStreamManager.ProfilingDataStream> existingDataStreams,
        DiscoveryNodes nodes,
        IndexMetadata.State state,
        boolean allShardsAssigned
    ) {
        Metadata.Builder metadataBuilder = Metadata.builder();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Map<String, IndexMetadata> indices = new HashMap<>();
        for (ProfilingDataStreamManager.ProfilingDataStream existingDataStream : existingDataStreams) {
            String writeIndexName = String.format(Locale.ROOT, ".ds-%s", existingDataStream.getName());
            Index writeIndex = new Index(writeIndexName, writeIndexName);
            DataStream ds = new DataStream(
                existingDataStream.getName(),
                List.of(writeIndex),
                1,
                Map.of(),
                false,
                false,
                false,
                false,
                IndexMode.STANDARD
            );
            metadataBuilder.put(ds);
            IndexMetadata.Builder builder = new IndexMetadata.Builder(writeIndexName);
            builder.state(state);
            builder.settings(indexSettings(IndexVersion.current(), 1, 1).put(IndexMetadata.SETTING_INDEX_UUID, writeIndex.getUUID()));
            builder.putMapping(
                new MappingMetadata(
                    MapperService.SINGLE_MAPPING_NAME,
                    Map.of(
                        "_meta",
                        Map.of(
                            "index-version",
                            existingDataStream.getVersion(),
                            "index-template-version",
                            ProfilingIndexTemplateRegistry.INDEX_TEMPLATE_VERSION
                        )
                    )
                )
            );
            builder.numberOfReplicas(0);
            builder.numberOfShards(1);
            IndexMetadata indexMetadata = builder.build();

            indices.put(writeIndexName, indexMetadata);
            ShardRouting shardRouting = ShardRouting.newUnassigned(
                new ShardId(writeIndex, 0),
                true,
                RecoverySource.ExistingStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""),
                ShardRouting.Role.DEFAULT
            );
            if (allShardsAssigned) {
                shardRouting = shardRouting.initialize("node0", null, 0).moveToStarted(0);
            }
            routingTableBuilder.add(
                IndexRoutingTable.builder(writeIndex)
                    .addIndexShard(IndexShardRoutingTable.builder(shardRouting.shardId()).addShard(shardRouting))
            );
        }

        return ClusterState.builder(new ClusterName("test"))
            .metadata(metadataBuilder.indices(indices).transientSettings(nodeSettings).build())
            .blocks(new ClusterBlocks.Builder().build())
            .nodes(nodes)
            .routingTable(routingTableBuilder)
            .build();
    }
}
