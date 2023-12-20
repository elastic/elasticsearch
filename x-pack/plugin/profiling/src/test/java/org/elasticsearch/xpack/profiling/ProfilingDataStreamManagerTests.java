/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
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
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;
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

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ProfilingDataStreamManagerTests extends ESTestCase {
    private final AtomicBoolean templatesCreated = new AtomicBoolean();
    private ProfilingDataStreamManager datastreamManager;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private VerifyingClient client;
    private List<ProfilingDataStreamManager.ProfilingDataStream> managedDataStreams;
    private int indexTemplateVersion;
    private IndexStateResolver indexStateResolver;

    @Before
    public void createRegistryAndClient() {
        templatesCreated.set(false);
        threadPool = new TestThreadPool(this.getClass().getName());
        client = new VerifyingClient(threadPool);
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        managedDataStreams = ProfilingDataStreamManager.PROFILING_DATASTREAMS;
        indexTemplateVersion = ProfilingIndexTemplateRegistry.INDEX_TEMPLATE_VERSION;
        indexStateResolver = new IndexStateResolver(true) {
            @Override
            protected int getIndexTemplateVersion() {
                return indexTemplateVersion;
            }
        };
        datastreamManager = new ProfilingDataStreamManager(threadPool, client, clusterService, indexStateResolver) {
            @Override
            protected boolean areAllIndexTemplatesCreated(ClusterChangedEvent event, Settings settings) {
                return templatesCreated.get();
            }

            @Override
            protected Iterable<ProfilingDataStream> getManagedIndices() {
                return managedDataStreams;
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
            IndexVersion.current(),
            false
        );

        AtomicInteger calledTimes = new AtomicInteger(0);

        client.setVerifier((action, request, listener) -> verifyDataStreamInstalled(calledTimes, action, request, listener));
        datastreamManager.clusterChanged(event);
        // should not create the index because a newer generation with the correct version exists
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(ProfilingDataStreamManager.PROFILING_DATASTREAMS.size() - 1)));

        calledTimes.set(0);
    }

    public void testThatOutdatedDataStreamIsDetectedIfCheckEnabled() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();
        templatesCreated.set(true);

        ProfilingDataStreamManager.ProfilingDataStream existingDataStream = randomFrom(ProfilingDataStreamManager.PROFILING_DATASTREAMS);
        ClusterChangedEvent event = createClusterChangedEvent(
            List.of(existingDataStream),
            nodes,
            IndexMetadata.State.OPEN,
            // This is an outdated version that requires indices to be deleted upon migration
            IndexVersions.V_8_8_2,
            true
        );

        AtomicInteger calledTimes = new AtomicInteger(0);

        client.setVerifier((action, request, listener) -> verifyDataStreamInstalled(calledTimes, action, request, listener));
        datastreamManager.clusterChanged(event);
        // should not create this index because the one that has changed is too old. Depending on the point at which the index is
        // evaluated, other indices may have already been created.
        assertBusy(
            () -> assertThat(
                calledTimes.get(),
                allOf(greaterThanOrEqualTo(0), Matchers.lessThan(ProfilingDataStreamManager.PROFILING_DATASTREAMS.size()))
            )
        );
        calledTimes.set(0);
    }

    public void testThatOutdatedDataStreamIsIgnoredIfCheckDisabled() throws Exception {
        // disable the check
        indexStateResolver.setCheckOutdatedIndices(false);

        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();
        templatesCreated.set(true);

        ProfilingDataStreamManager.ProfilingDataStream existingDataStream = randomFrom(ProfilingDataStreamManager.PROFILING_DATASTREAMS);
        ClusterChangedEvent event = createClusterChangedEvent(
            List.of(existingDataStream),
            nodes,
            IndexMetadata.State.OPEN,
            IndexVersions.V_8_8_2,
            true
        );

        AtomicInteger calledTimes = new AtomicInteger(0);

        client.setVerifier((action, request, listener) -> verifyDataStreamInstalled(calledTimes, action, request, listener));
        datastreamManager.clusterChanged(event);
        // should create all indices but consider the current one up-to-date
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
            IndexVersion.current(),
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

    public void testNoMigrationsIfIndexTemplateVersionMatches() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();
        templatesCreated.set(true);
        int nextIndexTemplateVersion = ProfilingIndexTemplateRegistry.INDEX_TEMPLATE_VERSION + 1;

        ProfilingDataStreamManager.ProfilingDataStream ds = ProfilingDataStreamManager.ProfilingDataStream.of(
            "profiling-test",
            1,
            new Migration.Builder().migrateToIndexTemplateVersion(nextIndexTemplateVersion).addProperty("test", "keyword")
        );

        managedDataStreams = List.of(ds);
        ClusterChangedEvent event = createClusterChangedEvent(managedDataStreams, nodes);

        client.setVerifier((a, r, l) -> {
            fail("all data streams should be up-to-date; nothing should happen");
            return null;
        });
        datastreamManager.clusterChanged(event);
    }

    public void testMigratesIfIndexTemplateVersionIsBehind() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();
        templatesCreated.set(true);
        int nextIndexTemplateVersion = ProfilingIndexTemplateRegistry.INDEX_TEMPLATE_VERSION + 1;

        ProfilingDataStreamManager.ProfilingDataStream ds = ProfilingDataStreamManager.ProfilingDataStream.of(
            "profiling-test",
            1,
            new Migration.Builder().migrateToIndexTemplateVersion(nextIndexTemplateVersion).addProperty("test", "keyword")
        );
        ProfilingDataStreamManager.ProfilingDataStream ds2 = ProfilingDataStreamManager.ProfilingDataStream.of("profiling-no-change", 1
        // no migration specified, should not be changed
        );

        managedDataStreams = List.of(ds, ds2);
        // index is out of date and should be migrated
        indexTemplateVersion = nextIndexTemplateVersion;
        ClusterChangedEvent event = createClusterChangedEvent(managedDataStreams, nodes);

        AtomicInteger mappingUpdates = new AtomicInteger(0);
        AtomicInteger settingsUpdates = new AtomicInteger(0);
        client.setVerifier(
            (action, request, listener) -> verifyIndexMigrated(
                ".ds-profiling-test",
                mappingUpdates,
                settingsUpdates,
                action,
                request,
                listener
            )
        );

        datastreamManager.clusterChanged(event);
        // one mapping update is the one we specified, the other one is because we need to update _meta
        assertBusy(() -> assertThat(mappingUpdates.get(), equalTo(2)));
        assertBusy(() -> assertThat(settingsUpdates.get(), equalTo(0)));

        mappingUpdates.set(0);
        settingsUpdates.set(0);
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
            RolloverRequest rolloverRequest = (RolloverRequest) request;
            return new RolloverResponse(
                rolloverRequest.getRolloverTarget(),
                rolloverRequest.getNewIndexName(),
                Map.of(),
                false,
                true,
                true,
                true
            );
        } else {
            fail("client called with unexpected request:" + request.toString());
            return null;
        }
    }

    private ActionResponse verifyIndexMigrated(
        String indexName,
        AtomicInteger mappingUpdates,
        AtomicInteger settingsUpdates,
        ActionType<?> action,
        ActionRequest request,
        ActionListener<?> listener
    ) {
        if (action instanceof PutMappingAction) {
            mappingUpdates.incrementAndGet();
            assertThat(action, instanceOf(PutMappingAction.class));
            assertThat(request, instanceOf(PutMappingRequest.class));
            assertThat(((PutMappingRequest) request).indices(), equalTo(new String[] { indexName }));
            assertNotNull(listener);
            return AcknowledgedResponse.TRUE;
        } else if (action instanceof UpdateSettingsAction) {
            settingsUpdates.incrementAndGet();
            assertThat(action, instanceOf(UpdateSettingsAction.class));
            assertThat(request, instanceOf(UpdateSettingsRequest.class));
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
        return createClusterChangedEvent(existingDataStreams, nodes, IndexMetadata.State.OPEN, IndexVersion.current(), true);
    }

    private ClusterChangedEvent createClusterChangedEvent(
        Iterable<ProfilingDataStreamManager.ProfilingDataStream> existingDataStreams,
        DiscoveryNodes nodes,
        IndexMetadata.State state,
        IndexVersion indexVersion,
        boolean allShardsAssigned
    ) {
        ClusterState cs = createClusterState(Settings.EMPTY, existingDataStreams, nodes, state, indexVersion, allShardsAssigned);
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
        IndexVersion indexVersion,
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
            builder.settings(indexSettings(indexVersion, 1, 1).put(IndexMetadata.SETTING_INDEX_UUID, writeIndex.getUUID()));
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
