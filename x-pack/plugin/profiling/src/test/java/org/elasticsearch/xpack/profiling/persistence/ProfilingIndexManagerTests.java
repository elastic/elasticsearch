/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.persistence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.admin.indices.settings.put.TransportUpdateSettingsAction;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ProfilingIndexManagerTests extends ESTestCase {
    private final AtomicBoolean templatesCreated = new AtomicBoolean();
    private ProfilingIndexManager indexManager;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private VerifyingClient client;
    private List<ProfilingIndexManager.ProfilingIndex> managedIndices;
    private int indexTemplateVersion;
    private IndexStateResolver indexStateResolver;

    @Before
    public void createRegistryAndClient() {
        templatesCreated.set(false);
        threadPool = new TestThreadPool(this.getClass().getName());
        client = new VerifyingClient(threadPool);
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
        managedIndices = ProfilingIndexManager.PROFILING_INDICES;
        indexTemplateVersion = ProfilingIndexTemplateRegistry.INDEX_TEMPLATE_VERSION;
        indexStateResolver = new IndexStateResolver(true) {
            @Override
            protected int getIndexTemplateVersion() {
                return indexTemplateVersion;
            }
        };
        indexManager = new ProfilingIndexManager(threadPool, client, clusterService, indexStateResolver) {
            @Override
            protected boolean areAllIndexTemplatesCreated(ClusterChangedEvent event, Settings settings) {
                return templatesCreated.get();
            }

            @Override
            protected Iterable<ProfilingIndex> getManagedIndices() {
                return managedIndices;
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

    public void testThatRedIndexIsNotTouched() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();
        templatesCreated.set(true);

        // This index is an upgrade candidate
        ProfilingIndexManager.ProfilingIndex existingIndex = randomFrom(ProfilingIndexManager.PROFILING_INDICES);
        ClusterChangedEvent event = createClusterChangedEvent(
            List.of(existingIndex.withVersion(0)),
            nodes,
            IndexMetadata.State.OPEN,
            IndexVersion.current(),
            false
        );

        AtomicInteger calledTimes = new AtomicInteger(0);

        client.setVerifier((action, request, listener) -> verifyIndexInstalled(calledTimes, action, request, listener));
        indexManager.clusterChanged(event);
        // should not create the index because a newer generation with the correct version exists
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(ProfilingIndexManager.PROFILING_INDICES.size() - 1)));

        calledTimes.set(0);
    }

    public void testThatOutdatedIndexIsDetectedIfCheckEnabled() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();
        templatesCreated.set(true);

        ProfilingIndexManager.ProfilingIndex existingIndex = randomFrom(ProfilingIndexManager.PROFILING_INDICES);
        ClusterChangedEvent event = createClusterChangedEvent(
            List.of(existingIndex.withVersion(0)),
            nodes,
            IndexMetadata.State.OPEN,
            // This is an outdated version that requires indices to be deleted upon migration
            IndexVersions.V_8_8_2,
            true
        );

        AtomicInteger calledTimes = new AtomicInteger(0);

        client.setVerifier((action, request, listener) -> verifyIndexInstalled(calledTimes, action, request, listener));
        indexManager.clusterChanged(event);
        // should not create this index because the one that has changed is too old. Depending on the point at which the index is
        // evaluated, other indices may have already been created.
        assertBusy(
            () -> assertThat(
                calledTimes.get(),
                allOf(greaterThanOrEqualTo(0), Matchers.lessThan(ProfilingIndexManager.PROFILING_INDICES.size()))
            )
        );
        calledTimes.set(0);
    }

    public void testThatOutdatedIndexIsIgnoredIfCheckDisabled() throws Exception {
        // disable the check
        indexStateResolver.setCheckOutdatedIndices(false);

        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();
        templatesCreated.set(true);

        ProfilingIndexManager.ProfilingIndex existingIndex = randomFrom(ProfilingIndexManager.PROFILING_INDICES);
        ClusterChangedEvent event = createClusterChangedEvent(
            List.of(existingIndex),
            nodes,
            IndexMetadata.State.OPEN,
            IndexVersions.V_8_8_2,
            true
        );

        AtomicInteger calledTimes = new AtomicInteger(0);

        client.setVerifier((action, request, listener) -> verifyIndexInstalled(calledTimes, action, request, listener));
        indexManager.clusterChanged(event);
        // should create all indices but consider the current one up-to-date
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(ProfilingIndexManager.PROFILING_INDICES.size() - 1)));
        calledTimes.set(0);
    }

    public void testThatClosedIndexIsNotTouched() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();
        templatesCreated.set(true);

        // This index is an upgrade candidate
        ProfilingIndexManager.ProfilingIndex existingIndex = randomFrom(ProfilingIndexManager.PROFILING_INDICES);
        ClusterChangedEvent event = createClusterChangedEvent(
            List.of(existingIndex.withVersion(0)),
            nodes,
            IndexMetadata.State.CLOSE,
            IndexVersion.current(),
            true
        );

        AtomicInteger calledTimes = new AtomicInteger(0);

        client.setVerifier((action, request, listener) -> verifyIndexInstalled(calledTimes, action, request, listener));
        indexManager.clusterChanged(event);
        // should not create the index because a newer generation with the correct version exists
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(ProfilingIndexManager.PROFILING_INDICES.size() - 1)));

        calledTimes.set(0);
    }

    public void testThatExistingIndicesAreNotCreatedTwice() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();
        templatesCreated.set(true);

        ProfilingIndexManager.ProfilingIndex existingIndex = randomFrom(ProfilingIndexManager.PROFILING_INDICES);
        ClusterChangedEvent event = createClusterChangedEvent(List.of(existingIndex), nodes);

        AtomicInteger calledTimes = new AtomicInteger(0);

        client.setVerifier((action, request, listener) -> verifyIndexInstalled(calledTimes, action, request, listener));
        indexManager.clusterChanged(event);
        // should not create the existing index
        assertBusy(() -> assertThat(calledTimes.get(), equalTo(ProfilingIndexManager.PROFILING_INDICES.size() - 1)));

        calledTimes.set(0);
    }

    public void testUpgradesOldIndex() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();
        templatesCreated.set(true);

        ProfilingIndexManager.ProfilingIndex indexWithDeleteOnVersionBump = randomFrom(
            ProfilingIndexManager.PROFILING_INDICES.stream()
                .filter(p -> p.getOnVersionBump().equals(ProfilingIndexManager.OnVersionBump.DELETE_OLD))
                .toList()
        );
        ProfilingIndexManager.ProfilingIndex oldIndex = indexWithDeleteOnVersionBump.withVersion(0);
        List<ProfilingIndexManager.ProfilingIndex> existingIndices = new ArrayList<>(ProfilingIndexManager.PROFILING_INDICES);
        // only the old index must exist
        existingIndices.remove(indexWithDeleteOnVersionBump);
        existingIndices.add(oldIndex);

        ClusterChangedEvent event = createClusterChangedEvent(existingIndices, nodes);

        AtomicInteger indicesCreated = new AtomicInteger(0);
        AtomicInteger indicesDeleted = new AtomicInteger(0);

        client.setVerifier((action, request, listener) -> verifyIndexUpgraded(indicesCreated, indicesDeleted, action, request, listener));
        indexManager.clusterChanged(event);
        // should delete one old index and create a new one
        assertBusy(() -> assertThat(indicesCreated.get(), equalTo(1)));
        assertBusy(() -> assertThat(indicesDeleted.get(), equalTo(1)));

        indicesCreated.set(0);
        indicesDeleted.set(0);
    }

    public void testNoMigrationsIfIndexTemplateVersionMatches() {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();
        templatesCreated.set(true);

        ProfilingIndexManager.ProfilingIndex idx = ProfilingIndexManager.ProfilingIndex.regular(
            "profiling-test",
            1,
            ProfilingIndexManager.OnVersionBump.KEEP_OLD,
            new Migration.Builder().migrateToIndexTemplateVersion(2).addProperty("test", "keyword")
        );

        managedIndices = List.of(idx);
        ClusterChangedEvent event = createClusterChangedEvent(managedIndices, nodes);

        client.setVerifier((a, r, l) -> {
            fail("all indices should be up-to-date; nothing should happen");
            return null;
        });
        indexManager.clusterChanged(event);
    }

    public void testMigratesIfIndexTemplateVersionIsBehind() throws Exception {
        DiscoveryNode node = DiscoveryNodeUtils.create("node");
        DiscoveryNodes nodes = DiscoveryNodes.builder().localNodeId("node").masterNodeId("node").add(node).build();
        templatesCreated.set(true);
        int nextIndexTemplateVersion = ProfilingIndexTemplateRegistry.INDEX_TEMPLATE_VERSION + 1;

        ProfilingIndexManager.ProfilingIndex idx = ProfilingIndexManager.ProfilingIndex.regular(
            "profiling-test",
            1,
            ProfilingIndexManager.OnVersionBump.KEEP_OLD,
            new Migration.Builder().migrateToIndexTemplateVersion(nextIndexTemplateVersion)
                .addProperty("test", "keyword")
                .dynamicSettings(
                    Settings.builder().put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(30)).build()
                )
        );
        ProfilingIndexManager.ProfilingIndex idx2 = ProfilingIndexManager.ProfilingIndex.regular(
            "profiling-no-change",
            1,
            ProfilingIndexManager.OnVersionBump.KEEP_OLD
            // no migration specified, should not be changed
        );

        managedIndices = List.of(idx, idx2);
        // index is out of date and should be migrated
        indexTemplateVersion = nextIndexTemplateVersion;
        ClusterChangedEvent event = createClusterChangedEvent(managedIndices, nodes);

        AtomicInteger mappingUpdates = new AtomicInteger(0);
        AtomicInteger settingsUpdates = new AtomicInteger(0);
        client.setVerifier(
            (action, request, listener) -> verifyIndexMigrated(
                ".profiling-test-v001",
                mappingUpdates,
                settingsUpdates,
                action,
                request,
                listener
            )
        );

        indexManager.clusterChanged(event);
        // one mapping update is the one we specified, the other one is because we need to update _meta
        assertBusy(() -> assertThat(mappingUpdates.get(), equalTo(2)));
        assertBusy(() -> assertThat(settingsUpdates.get(), equalTo(1)));

        mappingUpdates.set(0);
        settingsUpdates.set(0);
    }

    public void testIndexMatchWithoutVersion() {
        ProfilingIndexManager.ProfilingIndex idx = ProfilingIndexManager.ProfilingIndex.kv("profiling-test", 1);
        assertTrue(idx.isMatchWithoutVersion(".profiling-test-v002"));
        assertFalse(idx.isMatchWithoutVersion(".profiling-testing-v001"));
        assertFalse(idx.isMatchWithoutVersion(".profiling-test-verbose"));
        assertFalse(idx.isMatchWithoutVersion(".profiling-test-v"));
    }

    private ActionResponse verifyIndexInstalled(
        AtomicInteger calledTimes,
        ActionType<?> action,
        ActionRequest request,
        ActionListener<?> listener
    ) {
        if (action == TransportCreateIndexAction.TYPE) {
            calledTimes.incrementAndGet();
            assertThat(request, instanceOf(CreateIndexRequest.class));
            assertNotNull(listener);
            return new CreateIndexResponse(true, true, ((CreateIndexRequest) request).index());
        } else {
            fail("client called with unexpected request:" + request.toString());
            return null;
        }
    }

    private ActionResponse verifyIndexUpgraded(
        AtomicInteger indicesCreated,
        AtomicInteger indicesDeleted,
        ActionType<?> action,
        ActionRequest request,
        ActionListener<?> listener
    ) {
        if (action == TransportCreateIndexAction.TYPE) {
            indicesCreated.incrementAndGet();
            assertThat(request, instanceOf(CreateIndexRequest.class));
            assertNotNull(listener);
            return new CreateIndexResponse(true, true, ((CreateIndexRequest) request).index());
        } else if (action == TransportDeleteIndexAction.TYPE) {
            indicesDeleted.incrementAndGet();
            assertThat(request, instanceOf(DeleteIndexRequest.class));
            assertNotNull(listener);
            return AcknowledgedResponse.TRUE;
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
        if (action == TransportPutMappingAction.TYPE) {
            mappingUpdates.incrementAndGet();
            assertThat(request, instanceOf(PutMappingRequest.class));
            assertThat(((PutMappingRequest) request).indices(), equalTo(new String[] { indexName }));
            assertNotNull(listener);
            return AcknowledgedResponse.TRUE;
        } else if (action == TransportUpdateSettingsAction.TYPE) {
            settingsUpdates.incrementAndGet();
            assertThat(request, instanceOf(UpdateSettingsRequest.class));
            assertNotNull(listener);
            return AcknowledgedResponse.TRUE;
        } else {
            fail("client called with unexpected request:" + request.toString());
            return null;
        }
    }

    private ClusterChangedEvent createClusterChangedEvent(
        Iterable<ProfilingIndexManager.ProfilingIndex> existingIndices,
        DiscoveryNodes nodes
    ) {
        return createClusterChangedEvent(existingIndices, nodes, IndexMetadata.State.OPEN, IndexVersion.current(), true);
    }

    private ClusterChangedEvent createClusterChangedEvent(
        Iterable<ProfilingIndexManager.ProfilingIndex> existingIndices,
        DiscoveryNodes nodes,
        IndexMetadata.State state,
        IndexVersion indexVersion,
        boolean allShardsAssigned
    ) {
        ClusterState cs = createClusterState(Settings.EMPTY, existingIndices, nodes, state, indexVersion, allShardsAssigned);
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
        Iterable<ProfilingIndexManager.ProfilingIndex> existingIndices,
        DiscoveryNodes nodes,
        IndexMetadata.State state,
        IndexVersion indexVersion,
        boolean allShardsAssigned
    ) {
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Map<String, IndexMetadata> indices = new HashMap<>();
        for (ProfilingIndexManager.ProfilingIndex profilingIndex : existingIndices) {
            String indexName = profilingIndex.getName();
            Index index = new Index(indexName, indexName);
            IndexMetadata.Builder builder = new IndexMetadata.Builder(indexName);
            builder.state(state);
            builder.settings(indexSettings(indexVersion, 1, 1).put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID()));
            builder.putMapping(
                new MappingMetadata(
                    MapperService.SINGLE_MAPPING_NAME,
                    Map.of(
                        "_meta",
                        Map.of(
                            "index-version",
                            profilingIndex.getVersion(),
                            "index-template-version",
                            ProfilingIndexTemplateRegistry.INDEX_TEMPLATE_VERSION
                        )
                    )
                )
            );
            builder.numberOfReplicas(0);
            builder.numberOfShards(1);
            IndexMetadata indexMetadata = builder.build();

            indices.put(indexName, indexMetadata);
            ShardRouting shardRouting = ShardRouting.newUnassigned(
                new ShardId(index, 0),
                true,
                RecoverySource.ExistingStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""),
                ShardRouting.Role.DEFAULT
            );
            if (allShardsAssigned) {
                shardRouting = shardRouting.initialize("node0", null, 0).moveToStarted(0);
            }
            routingTableBuilder.add(
                IndexRoutingTable.builder(index)
                    .addIndexShard(IndexShardRoutingTable.builder(shardRouting.shardId()).addShard(shardRouting))
            );
        }

        return ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().indices(indices).transientSettings(nodeSettings).build())
            .blocks(new ClusterBlocks.Builder().build())
            .nodes(nodes)
            .routingTable(routingTableBuilder)
            .build();
    }
}
