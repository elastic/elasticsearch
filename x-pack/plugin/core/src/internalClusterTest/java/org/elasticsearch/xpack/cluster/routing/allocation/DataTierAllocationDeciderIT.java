/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.routing.allocation;

import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplanationUtils;
import org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesAction;
import org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DesiredNode;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.action.XPackUsageAction;
import org.elasticsearch.xpack.core.action.XPackUsageResponse;
import org.elasticsearch.xpack.core.datatiers.DataTiersFeatureSetUsage;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.elasticsearch.node.Node.NODE_EXTERNAL_ID_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.node.NodeRoleSettings.NODE_ROLES_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class DataTierAllocationDeciderIT extends ESIntegTestCase {
    private static final String index = "myindex";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(DataTierTelemetryPlugin.class);
    }

    @Before
    public void setUpMasterNode() {
        // Ensure that master nodes cannot hold any data
        internalCluster().startMasterOnlyNode();
    }

    public void testDefaultIndexAllocateToContent() {
        startWarmOnlyNode();
        startColdOnlyNode();
        ensureGreen();

        indicesAdmin().prepareCreate(index).setWaitForActiveShards(0).get();

        Settings idxSettings = indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT).addIndices(index).get().getSettings().get(index);
        assertThat(DataTier.TIER_PREFERENCE_SETTING.get(idxSettings), equalTo(DataTier.DATA_CONTENT));

        // index should be red
        assertThat(
            clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, index).get().getIndices().get(index).getStatus(),
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
        final var warmDesiredNode = desiredNode(randomAlphaOfLength(10), DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        final var coldDesiredNode = desiredNode(randomAlphaOfLength(15), DiscoveryNodeRole.DATA_COLD_NODE_ROLE);
        final var masterDesiredNode = desiredNode(internalCluster().getMasterName(), DiscoveryNodeRole.MASTER_ROLE);
        updateDesiredNodes(warmDesiredNode, coldDesiredNode, masterDesiredNode);

        startWarmOnlyNode(warmDesiredNode.externalId());
        final var coldNodeName = startColdOnlyNode(coldDesiredNode.externalId());

        createIndexWithTierPreference(DataTier.DATA_COLD, DataTier.DATA_WARM);

        ensureGreen(index);

        assertPrimaryShardIsAllocatedInNodeWithRole(0, DiscoveryNodeRole.DATA_COLD_NODE_ROLE);

        // Remove the cold tier
        updateDesiredNodes(masterDesiredNode, warmDesiredNode);

        assertBusy(() -> assertPrimaryShardIsAllocatedInNodeWithRole(0, DiscoveryNodeRole.DATA_WARM_NODE_ROLE));

        ensureGreen(index);
    }

    public void testShardsAreKeptInPreferredTierUntilTheNextTierIsInItsFinalState() throws Exception {
        final var hotDesiredNode = desiredNode("hot-node-0", DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
        final var warmDesiredNode = desiredNode("warn-node-0", DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        final var coldDesiredNode = desiredNode("cold-node-0", DiscoveryNodeRole.DATA_COLD_NODE_ROLE);
        final var masterDesiredNode = desiredNode(internalCluster().getMasterName(), DiscoveryNodeRole.MASTER_ROLE);
        updateDesiredNodes(hotDesiredNode, warmDesiredNode, coldDesiredNode, masterDesiredNode);

        startHotOnlyNode(hotDesiredNode.externalId());
        startWarmOnlyNode(warmDesiredNode.externalId());
        startColdOnlyNode(coldDesiredNode.externalId());

        createIndexWithTierPreference(DataTier.DATA_COLD, DataTier.DATA_WARM, DataTier.DATA_HOT);

        ensureGreen(index);

        assertPrimaryShardIsAllocatedInNodeWithRole(0, DiscoveryNodeRole.DATA_COLD_NODE_ROLE);

        final List<DesiredNode> newDesiredNodesInLeastPreferredTiers = new ArrayList<>();
        final var numberOfNewNodes = randomIntBetween(1, 5);
        for (int i = 1; i <= numberOfNewNodes; i++) {
            if (randomBoolean()) {
                newDesiredNodesInLeastPreferredTiers.add(desiredNode("hot-node-" + i, DiscoveryNodeRole.DATA_HOT_NODE_ROLE));
            } else {
                newDesiredNodesInLeastPreferredTiers.add(desiredNode("warm-node-" + i, DiscoveryNodeRole.DATA_WARM_NODE_ROLE));
            }
        }

        // Remove the cold tier and grow the next preferred tiers
        final List<DesiredNode> newDesiredNodes = new ArrayList<>(newDesiredNodesInLeastPreferredTiers);
        newDesiredNodes.add(masterDesiredNode);
        newDesiredNodes.add(hotDesiredNode);
        newDesiredNodes.add(warmDesiredNode);
        updateDesiredNodes(newDesiredNodes);

        ensureGreen(index);

        assertBusy(() -> assertPrimaryShardIsAllocatedInNodeWithRole(0, DiscoveryNodeRole.DATA_COLD_NODE_ROLE));

        for (final var newDesiredNode : newDesiredNodesInLeastPreferredTiers) {
            if (newDesiredNode.getRoles().contains(DiscoveryNodeRole.DATA_HOT_NODE_ROLE)) {
                startHotOnlyNode(newDesiredNode.externalId());
            } else {
                startWarmOnlyNode(newDesiredNode.externalId());
            }
        }

        ensureGreen(index);

        assertBusy(() -> assertPrimaryShardIsAllocatedInNodeWithRole(0, DiscoveryNodeRole.DATA_WARM_NODE_ROLE));
    }

    public void testSimpleAllocationDecisionWithDesiredNodes() {
        final var warmDesiredNode = desiredNode("warn-node-0", DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        final var warmDesiredNode2 = desiredNode("warn-node-1", DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        final var masterDesiredNode = desiredNode(internalCluster().getMasterName(), DiscoveryNodeRole.MASTER_ROLE);
        updateDesiredNodes(warmDesiredNode, warmDesiredNode2, masterDesiredNode);

        startWarmOnlyNode(warmDesiredNode.externalId());

        createIndexWithTierPreference(DataTier.DATA_COLD, DataTier.DATA_WARM);

        ensureGreen(index);

        assertPrimaryShardIsAllocatedInNodeWithRole(0, DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
    }

    public void testGrowAndShrinkSingleNodeInTier() throws Exception {
        final var warmDesiredNode = desiredNode("warm-node", DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
        final var coldDesiredNode = desiredNode("cold-node-1", DiscoveryNodeRole.DATA_COLD_NODE_ROLE);
        final var masterDesiredNode = desiredNode(internalCluster().getMasterName(), DiscoveryNodeRole.MASTER_ROLE);
        updateDesiredNodes(warmDesiredNode, coldDesiredNode, masterDesiredNode);

        startWarmOnlyNode(warmDesiredNode.externalId());
        var coldNodeName = startColdOnlyNode(coldDesiredNode.externalId());

        createIndexWithTierPreference(DataTier.DATA_COLD, DataTier.DATA_WARM);

        ensureGreen(index);

        assertPrimaryShardIsAllocatedInNodeWithRole(0, DiscoveryNodeRole.DATA_COLD_NODE_ROLE);

        final var newColdDesiredNode = desiredNode("cold-node-2", DiscoveryNodeRole.DATA_COLD_NODE_ROLE);
        updateDesiredNodes(warmDesiredNode, newColdDesiredNode, masterDesiredNode);

        // Exclude the node that we want to decommission, so it can move to the new cold node
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", coldNodeName), index);
        assertBusy(() -> assertPrimaryShardIsAllocatedInNodeWithRole(0, DiscoveryNodeRole.DATA_COLD_NODE_ROLE));

        startColdOnlyNode(newColdDesiredNode.externalId());

        ensureGreen(index);

        assertBusy(() -> assertPrimaryShardIsAllocatedInNode(0, newColdDesiredNode));

        internalCluster().stopNode(coldNodeName);

        ensureGreen(index);
    }

    public void testDesiredNodesAreTakenIntoAccountInAutoExpandReplicas() throws Exception {
        final var masterDesiredNode = desiredNode(internalCluster().getMasterName(), DiscoveryNodeRole.MASTER_ROLE);
        final int numberOfColdNodes = randomIntBetween(2, 5);
        final List<DesiredNode> coldDesiredNodes = new ArrayList<>();
        for (int i = 0; i < numberOfColdNodes; i++) {
            final var coldDesiredNode = desiredNode("cold-node-" + i, DiscoveryNodeRole.DATA_COLD_NODE_ROLE);
            coldDesiredNodes.add(coldDesiredNode);
            startColdOnlyNode(coldDesiredNode.externalId());
        }
        final int numberOfWarmNodes = randomIntBetween(numberOfColdNodes + 1, 10);
        final List<DesiredNode> warmDesiredNodes = new ArrayList<>();
        for (int i = 0; i < numberOfWarmNodes; i++) {
            final var warmDesiredNode = desiredNode("warm-node-" + i, DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
            warmDesiredNodes.add(warmDesiredNode);
            startWarmOnlyNode(warmDesiredNode.externalId());
        }
        final List<DesiredNode> desiredNodesWithWarmAndColdTier = new ArrayList<>();
        desiredNodesWithWarmAndColdTier.addAll(warmDesiredNodes);
        desiredNodesWithWarmAndColdTier.addAll(coldDesiredNodes);
        desiredNodesWithWarmAndColdTier.add(masterDesiredNode);

        updateDesiredNodes(desiredNodesWithWarmAndColdTier);

        indicesAdmin().prepareCreate(index)
            .setWaitForActiveShards(0)
            .setSettings(
                Settings.builder()
                    .put(DataTier.TIER_PREFERENCE, String.join(",", DataTier.DATA_COLD, DataTier.DATA_WARM))
                    .put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
                    .put(INDEX_AUTO_EXPAND_REPLICAS_SETTING.getKey(), "0-all")
            )
            .get();

        var replicas = indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT)
            .setIndices(index)
            .get()
            .getSetting(index, INDEX_NUMBER_OF_REPLICAS_SETTING.getKey());

        assertThat(Integer.parseInt(replicas), is(equalTo(numberOfColdNodes - 1)));

        final List<DesiredNode> desiredNodesWithoutColdTier = new ArrayList<>(warmDesiredNodes);
        desiredNodesWithoutColdTier.add(masterDesiredNode);

        updateDesiredNodes(desiredNodesWithoutColdTier);

        assertBusy(() -> {
            var newReplicaCount = indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT)
                .setIndices(index)
                .get()
                .getSetting(index, INDEX_NUMBER_OF_REPLICAS_SETTING.getKey());

            assertThat(Integer.parseInt(newReplicaCount), is(equalTo(numberOfWarmNodes - 1)));
        });
    }

    public void testOverrideDefaultAllocation() {
        startWarmOnlyNode();
        startColdOnlyNode();
        ensureGreen();

        indicesAdmin().prepareCreate(index)
            .setWaitForActiveShards(0)
            .setSettings(Settings.builder().put(DataTier.TIER_PREFERENCE, DataTier.DATA_WARM))
            .get();

        Settings idxSettings = indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT).addIndices(index).get().getSettings().get(index);
        assertThat(idxSettings.get(DataTier.TIER_PREFERENCE), equalTo(DataTier.DATA_WARM));

        // index should be yellow
        logger.info("--> waiting for {} to be yellow", index);
        ensureYellow(index);
    }

    public void testRequestSettingOverridden() {
        startContentOnlyNode();
        ensureGreen();

        indicesAdmin().prepareCreate(index)
            .setWaitForActiveShards(0)
            .setSettings(Settings.builder().putNull(DataTier.TIER_PREFERENCE)) // will be overridden to data_content
            .get();

        Settings idxSettings = indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT).addIndices(index).get().getSettings().get(index);
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

        indicesAdmin().prepareCreate(index)
            .setWaitForActiveShards(0)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(DataTier.TIER_PREFERENCE, "data_warm")
            )
            .get();

        indicesAdmin().prepareAddBlock(IndexMetadata.APIBlock.READ_ONLY, index).get();
        indicesAdmin().prepareResizeIndex(index, index + "-shrunk")
            .setResizeType(ResizeType.SHRINK)
            .setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
            )
            .get();

        ensureGreen(index + "-shrunk");

        Settings idxSettings = indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT)
            .addIndices(index + "-shrunk")
            .get()
            .getSettings()
            .get(index + "-shrunk");
        // It should inherit the setting of its originator
        assertThat(DataTier.TIER_PREFERENCE_SETTING.get(idxSettings), equalTo(DataTier.DATA_WARM));

        // Required or else the test cleanup fails because it can't delete the indices
        updateIndexSettings(Settings.builder().put("index.blocks.read_only", false), index, index + "-shrunk");
    }

    public void testTemplateOverridden() {
        startContentOnlyNode();

        Template t = new Template(Settings.builder().putNull(DataTier.TIER_PREFERENCE).build(), null, null);
        ComposableIndexTemplate ct = ComposableIndexTemplate.builder().indexPatterns(Collections.singletonList(index)).template(t).build();
        client().execute(
            TransportPutComposableIndexTemplateAction.TYPE,
            new TransportPutComposableIndexTemplateAction.Request("template").indexTemplate(ct)
        ).actionGet();

        indicesAdmin().prepareCreate(index).setWaitForActiveShards(0).get();

        Settings idxSettings = indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT).addIndices(index).get().getSettings().get(index);
        assertThat(DataTier.TIER_PREFERENCE_SETTING.get(idxSettings), equalTo("data_content"));

        // index should be yellow
        ensureYellow(index);
    }

    public void testDataTierTelemetry() {
        startContentOnlyNode();
        startContentOnlyNode();
        startHotOnlyNode();

        indicesAdmin().prepareCreate(index)
            .setSettings(indexSettings(2, 0).put(DataTier.TIER_PREFERENCE, "data_hot"))
            .setWaitForActiveShards(0)
            .get();

        indicesAdmin().prepareCreate(index + "2").setSettings(indexSettings(1, 1)).setWaitForActiveShards(0).get();

        ensureGreen();
        prepareIndex(index).setSource("foo", "bar").get();
        prepareIndex(index + "2").setSource("foo", "bar").get();
        prepareIndex(index + "2").setSource("foo", "bar").get();
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
            () -> createIndex(index, indexSettings(1, 0).put(DataTier.TIER_PREFERENCE, DataTier.DATA_FROZEN).build())
        );
        assertThat(e.getMessage(), equalTo("[data_frozen] tier can only be used for partial searchable snapshots"));

        String initialTier = randomFrom(DataTier.DATA_HOT, DataTier.DATA_WARM, DataTier.DATA_COLD);
        createIndex(index, indexSettings(1, 0).put(DataTier.TIER_PREFERENCE, initialTier).build());

        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> updatePreference(DataTier.DATA_FROZEN));
        assertThat(e2.getMessage(), equalTo("[data_frozen] tier can only be used for partial searchable snapshots"));

        updatePreference(randomValueOtherThan(initialTier, () -> randomFrom(DataTier.DATA_HOT, DataTier.DATA_WARM, DataTier.DATA_COLD)));
    }

    private void updatePreference(String tier) {
        updateIndexSettings(Settings.builder().put(DataTier.TIER_PREFERENCE, tier), index);
    }

    private DataTiersFeatureSetUsage getUsage() {
        XPackUsageResponse usages = safeGet(client().execute(XPackUsageAction.INSTANCE, new XPackUsageRequest(SAFE_AWAIT_TIMEOUT)));
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
        startHotOnlyNode(null);
    }

    public void startHotOnlyNode(@Nullable String externalId) {
        Settings.Builder nodeSettings = Settings.builder()
            .putList("node.roles", Arrays.asList("master", "data_hot", "ingest"))
            .put("node.attr.box", "hot");

        if (externalId != null) {
            nodeSettings.put(NODE_EXTERNAL_ID_SETTING.getKey(), externalId);
        }

        internalCluster().startNode(nodeSettings.build());
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
        return internalCluster().startNode(nodeSettings.build());
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

        return internalCluster().startNode(nodeSettings.build());
    }

    public void startFrozenOnlyNode() {
        Settings nodeSettings = Settings.builder()
            .putList("node.roles", Arrays.asList("master", "data_frozen", "ingest"))
            .put("node.attr.box", "frozen")
            .build();
        internalCluster().startNode(nodeSettings);
    }

    private DesiredNode desiredNode(String externalId, DiscoveryNodeRole... roles) {
        assertThat(roles.length, is(greaterThan(0)));

        final var nodeRoles = Arrays.stream(roles).map(DiscoveryNodeRole::roleName).collect(Collectors.joining(","));
        final var settings = Settings.builder()
            .put(NODE_ROLES_SETTING.getKey(), nodeRoles)
            .put(NODE_EXTERNAL_ID_SETTING.getKey(), externalId)
            .put(NODE_NAME_SETTING.getKey(), externalId)
            .build();
        return new DesiredNode(settings, 1, ByteSizeValue.ONE, ByteSizeValue.ONE);
    }

    private void updateDesiredNodes(DesiredNode... desiredNodes) {
        assertThat(desiredNodes.length, is(greaterThan(0)));
        updateDesiredNodes(Arrays.asList(desiredNodes));
    }

    private void updateDesiredNodes(List<DesiredNode> desiredNodes) {
        assertThat(desiredNodes.size(), is(greaterThan(0)));

        final var request = new UpdateDesiredNodesRequest(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            randomAlphaOfLength(10),
            1,
            desiredNodes,
            false
        );
        internalCluster().client().execute(UpdateDesiredNodesAction.INSTANCE, request).actionGet();
    }

    private void assertPrimaryShardIsAllocatedInNodeWithRole(int shard, DiscoveryNodeRole expectedRole) {
        final var discoveryNode = getPrimaryShardAssignedNode(shard);
        assertThat(explainAllocation(shard), discoveryNode.getRoles().contains(expectedRole), is(true));
    }

    private void assertPrimaryShardIsAllocatedInNode(int shard, DesiredNode expectedNode) {
        final var discoveryNode = getPrimaryShardAssignedNode(shard);
        assertThat(explainAllocation(shard), discoveryNode.getExternalId(), is(equalTo(expectedNode.externalId())));
    }

    private DiscoveryNode getPrimaryShardAssignedNode(int shard) {
        final var state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        final var routingTable = state.routingTable().index(index).shard(shard);
        final var primaryShard = routingTable.primaryShard();
        final var discoveryNode = state.nodes().get(primaryShard.currentNodeId());
        assertThat(discoveryNode, is(notNullValue()));
        return discoveryNode;
    }

    private String explainAllocation(int shard) {
        return Strings.toString(
            ClusterAllocationExplanationUtils.getClusterAllocationExplanation(client(), index, shard, true),
            true,
            true
        );
    }

    private void createIndexWithTierPreference(String... tiers) {
        assertThat(tiers.length, is(greaterThan(0)));

        indicesAdmin().prepareCreate(index)
            .setWaitForActiveShards(0)
            .setSettings(
                Settings.builder().put(DataTier.TIER_PREFERENCE, String.join(",", tiers)).put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            )
            .get();
    }
}
