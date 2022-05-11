/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesAction;
import org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DesiredNode;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.DataTiersFeatureSetUsage;
import org.elasticsearch.xpack.core.action.XPackUsageRequestBuilder;
import org.elasticsearch.xpack.core.action.XPackUsageResponse;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.elasticsearch.node.Node.NODE_EXTERNAL_ID_SETTING;
import static org.elasticsearch.node.NodeRoleSettings.NODE_ROLES_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class DataTierAllocationDeciderIT extends ESIntegTestCase {
    private static final String index = "myindex";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(DataTierTelemetryPlugin.class);
    }

    public void testDefaultIndexAllocateToContent() {
        startWarmOnlyNode();
        startColdOnlyNode();
        ensureGreen();

        client().admin().indices().prepareCreate(index).setWaitForActiveShards(0).get();

        Settings idxSettings = client().admin().indices().prepareGetIndex().addIndices(index).get().getSettings().get(index);
        assertThat(DataTier.TIER_PREFERENCE_SETTING.get(idxSettings), equalTo(DataTier.DATA_CONTENT));

        // index should be red
        assertThat(
            client().admin().cluster().prepareHealth(index).get().getIndices().get(index).getStatus(),
            equalTo(ClusterHealthStatus.RED)
        );

        if (randomBoolean()) {
            logger.info("--> starting content node");
            startContentOnlyNode();
        } else {
            logger.info("--> starting data node");
            startDataNode();
        }

        logger.info("--> waiting for {} to be yellow", index);
        ensureYellow(index);
    }

    public void testDesiredNodesAreConsideredDuringAllocation() throws Exception {
        final var initialNodes = client().admin().cluster().prepareState().get().getState().nodes().stream().toList();

        assertThat(initialNodes.size(), is(equalTo(1)));

        final var masterNode = initialNodes.get(0);

        final var warmDesiredNode = desiredNode(randomAlphaOfLength(10), DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        final var coldDesiredNode = desiredNode(randomAlphaOfLength(15), DiscoveryNodeRole.DATA_COLD_NODE_ROLE);
        final var masterDesiredNode = desiredNode(masterNode.getName(), DiscoveryNodeRole.MASTER_ROLE);
        updateDesiredNodes(warmDesiredNode, coldDesiredNode, masterDesiredNode);

        startWarmOnlyNode(warmDesiredNode.externalId());
        final var coldNodeName = startColdOnlyNode(coldDesiredNode.externalId());

        client().admin()
            .indices()
            .prepareCreate(index)
            .setWaitForActiveShards(0)
            .setSettings(
                Settings.builder()
                    .put(DataTier.TIER_PREFERENCE, String.join(",", DataTier.DATA_COLD, DataTier.DATA_WARM))
                    .put("index.routing.allocation.exclude._name", masterNode.getName()) // Exclude nodes we're not interested in
                    .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            )
            .get();

        ensureGreen(index);

        assertPrimaryShardIsAllocatedInNode(0, coldDesiredNode);

        // Remove the cold tier
        updateDesiredNodes(masterDesiredNode, warmDesiredNode);

        client().admin()
            .indices()
            .prepareUpdateSettings(index)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.exclude._name", String.join(",", masterNode.getName(), coldNodeName))
                    .build()
            )
            .get();

        assertBusy(() -> assertPrimaryShardIsAllocatedInNode(0, warmDesiredNode));

        ensureGreen(index);
    }

    public void testGrowAndShrinkSingleNodeInTier() throws Exception {
        final var initialNodes = client().admin().cluster().prepareState().get().getState().nodes().stream().toList();

        assertThat(initialNodes.size(), is(equalTo(1)));

        final var masterNode = initialNodes.get(0);

        final var warmDesiredNode = desiredNode(randomAlphaOfLength(10), DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        final var coldDesiredNode = desiredNode(randomAlphaOfLength(15), DiscoveryNodeRole.DATA_COLD_NODE_ROLE);
        final var masterDesiredNode = desiredNode(masterNode.getName(), DiscoveryNodeRole.MASTER_ROLE);
        updateDesiredNodes(warmDesiredNode, coldDesiredNode, masterDesiredNode);

        startWarmOnlyNode(warmDesiredNode.externalId());
        final var coldNodeName = startColdOnlyNode(coldDesiredNode.externalId());

        client().admin()
            .indices()
            .prepareCreate(index)
            .setWaitForActiveShards(0)
            .setSettings(
                Settings.builder()
                    .put(DataTier.TIER_PREFERENCE, String.join(",", DataTier.DATA_COLD, DataTier.DATA_WARM))
                    .put("index.routing.allocation.exclude._name", masterNode.getName()) // Exclude nodes we're not interested in
                    .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            )
            .get();

        ensureGreen(index);

        assertPrimaryShardIsAllocatedInNode(0, coldDesiredNode);

        final var newColdDesiredNode = desiredNode(randomAlphaOfLength(15), DiscoveryNodeRole.DATA_COLD_NODE_ROLE);
        updateDesiredNodes(warmDesiredNode, newColdDesiredNode, masterDesiredNode);

        client().admin()
            .indices()
            .prepareUpdateSettings(index)
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.exclude._name", String.join(",", masterNode.getName(), coldNodeName))
                    .build()
            )
            .get();

        assertBusy(() -> assertPrimaryShardIsAllocatedInNode(0, coldDesiredNode));

        startColdOnlyNode(newColdDesiredNode.externalId());

        assertBusy(() -> assertPrimaryShardIsAllocatedInNode(0, newColdDesiredNode));

        ensureGreen(index);
    }

    public void testOverrideDefaultAllocation() {
        startWarmOnlyNode();
        startColdOnlyNode();
        ensureGreen();

        client().admin()
            .indices()
            .prepareCreate(index)
            .setWaitForActiveShards(0)
            .setSettings(Settings.builder().put(DataTier.TIER_PREFERENCE, DataTier.DATA_WARM))
            .get();

        Settings idxSettings = client().admin().indices().prepareGetIndex().addIndices(index).get().getSettings().get(index);
        assertThat(idxSettings.get(DataTier.TIER_PREFERENCE), equalTo(DataTier.DATA_WARM));

        // index should be yellow
        logger.info("--> waiting for {} to be yellow", index);
        ensureYellow(index);
    }

    public void testRequestSettingOverridden() {
        startContentOnlyNode();
        ensureGreen();

        client().admin()
            .indices()
            .prepareCreate(index)
            .setWaitForActiveShards(0)
            .setSettings(Settings.builder().putNull(DataTier.TIER_PREFERENCE)) // will be overridden to data_content
            .get();

        Settings idxSettings = client().admin().indices().prepareGetIndex().addIndices(index).get().getSettings().get(index);
        assertThat(DataTier.TIER_PREFERENCE_SETTING.get(idxSettings), equalTo("data_content"));

        // index should be yellow
        logger.info("--> waiting for {} to be yellow", index);
        ensureYellow(index);
    }

    /**
     * When a new index is created from source metadata (as during a shrink), the data tier
     * default setting should *not* be applied. This test checks that behavior.
     */
    public void testShrinkStaysOnTier() {
        startWarmOnlyNode();
        startHotOnlyNode();

        client().admin()
            .indices()
            .prepareCreate(index)
            .setWaitForActiveShards(0)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(DataTier.TIER_PREFERENCE, "data_warm")
            )
            .get();

        client().admin().indices().prepareAddBlock(IndexMetadata.APIBlock.READ_ONLY, index).get();
        client().admin()
            .indices()
            .prepareResizeIndex(index, index + "-shrunk")
            .setResizeType(ResizeType.SHRINK)
            .setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
            )
            .get();

        ensureGreen(index + "-shrunk");

        Settings idxSettings = client().admin()
            .indices()
            .prepareGetIndex()
            .addIndices(index + "-shrunk")
            .get()
            .getSettings()
            .get(index + "-shrunk");
        // It should inherit the setting of its originator
        assertThat(DataTier.TIER_PREFERENCE_SETTING.get(idxSettings), equalTo(DataTier.DATA_WARM));

        // Required or else the test cleanup fails because it can't delete the indices
        client().admin()
            .indices()
            .prepareUpdateSettings(index, index + "-shrunk")
            .setSettings(Settings.builder().put("index.blocks.read_only", false))
            .get();
    }

    public void testTemplateOverridden() {
        startContentOnlyNode();

        Template t = new Template(Settings.builder().putNull(DataTier.TIER_PREFERENCE).build(), null, null);
        ComposableIndexTemplate ct = new ComposableIndexTemplate.Builder().indexPatterns(Collections.singletonList(index))
            .template(t)
            .build();
        client().execute(
            PutComposableIndexTemplateAction.INSTANCE,
            new PutComposableIndexTemplateAction.Request("template").indexTemplate(ct)
        ).actionGet();

        client().admin().indices().prepareCreate(index).setWaitForActiveShards(0).get();

        Settings idxSettings = client().admin().indices().prepareGetIndex().addIndices(index).get().getSettings().get(index);
        assertThat(DataTier.TIER_PREFERENCE_SETTING.get(idxSettings), equalTo("data_content"));

        // index should be yellow
        ensureYellow(index);
    }

    public void testDataTierTelemetry() {
        startContentOnlyNode();
        startContentOnlyNode();
        startHotOnlyNode();

        client().admin()
            .indices()
            .prepareCreate(index)
            .setSettings(
                Settings.builder()
                    .put(DataTier.TIER_PREFERENCE, "data_hot")
                    .put("index.number_of_shards", 2)
                    .put("index.number_of_replicas", 0)
            )
            .setWaitForActiveShards(0)
            .get();

        client().admin()
            .indices()
            .prepareCreate(index + "2")
            .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 1))
            .setWaitForActiveShards(0)
            .get();

        ensureGreen();
        client().prepareIndex(index).setSource("foo", "bar").get();
        client().prepareIndex(index + "2").setSource("foo", "bar").get();
        client().prepareIndex(index + "2").setSource("foo", "bar").get();
        refresh(index, index + "2");

        DataTiersFeatureSetUsage usage = getUsage();
        // We can't guarantee that internal indices aren't created, so some of these are >= checks
        assertThat(usage.getTierStats().get(DataTier.DATA_CONTENT).nodeCount, equalTo(2));
        assertThat(usage.getTierStats().get(DataTier.DATA_CONTENT).indexCount, greaterThanOrEqualTo(1));
        assertThat(usage.getTierStats().get(DataTier.DATA_CONTENT).totalShardCount, greaterThanOrEqualTo(2));
        assertThat(usage.getTierStats().get(DataTier.DATA_CONTENT).primaryShardCount, greaterThanOrEqualTo(1));
        assertThat(usage.getTierStats().get(DataTier.DATA_CONTENT).docCount, greaterThanOrEqualTo(2L));
        assertThat(usage.getTierStats().get(DataTier.DATA_CONTENT).primaryByteCount, greaterThanOrEqualTo(1L));
        assertThat(usage.getTierStats().get(DataTier.DATA_CONTENT).primaryByteCountMedian, greaterThanOrEqualTo(1L));
        assertThat(usage.getTierStats().get(DataTier.DATA_CONTENT).primaryShardBytesMAD, greaterThanOrEqualTo(0L));
        assertThat(usage.getTierStats().get(DataTier.DATA_HOT).nodeCount, equalTo(1));
        assertThat(usage.getTierStats().get(DataTier.DATA_HOT).indexCount, greaterThanOrEqualTo(1));
        assertThat(usage.getTierStats().get(DataTier.DATA_HOT).totalShardCount, greaterThanOrEqualTo(2));
        assertThat(usage.getTierStats().get(DataTier.DATA_HOT).primaryShardCount, greaterThanOrEqualTo(2));
        assertThat(usage.getTierStats().get(DataTier.DATA_HOT).docCount, greaterThanOrEqualTo(1L));
        assertThat(usage.getTierStats().get(DataTier.DATA_HOT).primaryByteCount, greaterThanOrEqualTo(1L));
        assertThat(usage.getTierStats().get(DataTier.DATA_HOT).primaryByteCountMedian, greaterThanOrEqualTo(1L));
        assertThat(usage.getTierStats().get(DataTier.DATA_HOT).primaryShardBytesMAD, greaterThanOrEqualTo(0L));
    }

    public void testIllegalOnFrozen() {
        startDataNode();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createIndex(
                index,
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put(DataTier.TIER_PREFERENCE, DataTier.DATA_FROZEN)
                    .build()
            )
        );
        assertThat(e.getMessage(), equalTo("[data_frozen] tier can only be used for partial searchable snapshots"));

        String initialTier = randomFrom(DataTier.DATA_HOT, DataTier.DATA_WARM, DataTier.DATA_COLD);
        createIndex(
            index,
            Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put(DataTier.TIER_PREFERENCE, initialTier)
                .build()
        );

        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> updatePreference(DataTier.DATA_FROZEN));
        assertThat(e2.getMessage(), equalTo("[data_frozen] tier can only be used for partial searchable snapshots"));

        updatePreference(randomValueOtherThan(initialTier, () -> randomFrom(DataTier.DATA_HOT, DataTier.DATA_WARM, DataTier.DATA_COLD)));
    }

    private void updatePreference(String tier) {
        client().admin()
            .indices()
            .updateSettings(new UpdateSettingsRequest(index).settings(Map.of(DataTier.TIER_PREFERENCE, tier)))
            .actionGet();
    }

    private DataTiersFeatureSetUsage getUsage() {
        XPackUsageResponse usages = new XPackUsageRequestBuilder(client()).execute().actionGet();
        return usages.getUsages()
            .stream()
            .filter(u -> u instanceof DataTiersFeatureSetUsage)
            .findFirst()
            .map(u -> (DataTiersFeatureSetUsage) u)
            .orElseThrow();
    }

    public void startDataNode() {
        Settings nodeSettings = Settings.builder()
            .putList("node.roles", Arrays.asList("master", "data", "ingest"))
            .put("node.attr.box", "all")
            .build();
        internalCluster().startNode(nodeSettings);
    }

    public void startContentOnlyNode() {
        Settings nodeSettings = Settings.builder()
            .putList("node.roles", Arrays.asList("master", "data_content", "ingest"))
            .put("node.attr.box", "content")
            .build();
        internalCluster().startNode(nodeSettings);
    }

    public void startHotOnlyNode() {
        Settings nodeSettings = Settings.builder()
            .putList("node.roles", Arrays.asList("master", "data_hot", "ingest"))
            .put("node.attr.box", "hot")
            .build();
        internalCluster().startNode(nodeSettings);
    }

    public void startWarmOnlyNode() {
        startWarmOnlyNode(null);
    }

    public String startWarmOnlyNode(@Nullable String externalId) {
        Settings.Builder nodeSettings = Settings.builder()
            .putList("node.roles", Arrays.asList("master", "data_warm", "ingest"))
            .put("node.attr.box", "warm");

        if (externalId != null) {
            nodeSettings.put(NODE_EXTERNAL_ID_SETTING.getKey(), externalId);
        }
        return internalCluster().startNode(nodeSettings);
    }

    public void startColdOnlyNode() {
        startColdOnlyNode(null);
    }

    public String startColdOnlyNode(@Nullable String externalId) {
        Settings.Builder nodeSettings = Settings.builder()
            .putList("node.roles", Arrays.asList("master", "data_cold", "ingest"))
            .put("node.attr.box", "cold");

        if (externalId != null) {
            nodeSettings.put(NODE_EXTERNAL_ID_SETTING.getKey(), externalId);
        }

        return internalCluster().startNode(nodeSettings);
    }

    public void startFrozenOnlyNode() {
        Settings nodeSettings = Settings.builder()
            .putList("node.roles", Arrays.asList("master", "data_frozen", "ingest"))
            .put("node.attr.box", "frozen")
            .build();
        internalCluster().startNode(nodeSettings);
    }

    private DesiredNode desiredNode(DiscoveryNodeRole... roles) {
        assertThat(roles.length, is(greaterThan(0)));

        final var nodeRoles = Arrays.stream(roles).map(DiscoveryNodeRole::roleName).collect(Collectors.joining(","));
        final var settings = Settings.builder()
            .put(NODE_ROLES_SETTING.getKey(), nodeRoles)
            .put(NODE_EXTERNAL_ID_SETTING.getKey(), randomAlphaOfLength(14))
            .build();
        return new DesiredNode(settings, 1, ByteSizeValue.ONE, ByteSizeValue.ONE, Version.CURRENT);
    }

    private DesiredNode desiredNode(String externalId, DiscoveryNodeRole... roles) {
        assertThat(roles.length, is(greaterThan(0)));

        final var nodeRoles = Arrays.stream(roles).map(DiscoveryNodeRole::roleName).collect(Collectors.joining(","));
        final var settings = Settings.builder()
            .put(NODE_ROLES_SETTING.getKey(), nodeRoles)
            .put(NODE_EXTERNAL_ID_SETTING.getKey(), externalId)
            .build();
        return new DesiredNode(settings, 1, ByteSizeValue.ONE, ByteSizeValue.ONE, Version.CURRENT);
    }

    private void updateDesiredNodes(DesiredNode... desiredNodes) {
        assertThat(desiredNodes.length, is(greaterThan(0)));

        final var request = new UpdateDesiredNodesRequest(randomAlphaOfLength(10), 1, Arrays.asList(desiredNodes));
        internalCluster().client().execute(UpdateDesiredNodesAction.INSTANCE, request).actionGet();
    }

    protected void assertPrimaryShardIsAllocatedInNode(int shard, DesiredNode expectedNode) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        IndexShardRoutingTable routingTable = state.routingTable().index(index).shard(shard);
        ShardRouting primaryShard = routingTable.primaryShard();
        DiscoveryNode discoveryNode = state.nodes().get(primaryShard.currentNodeId());
        assertThat(discoveryNode.toString(), discoveryNode.getExternalId(), is(equalTo(expectedNode.externalId())));
    }
}
