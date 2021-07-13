/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.routing.allocation;

import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.DataTier;
import org.elasticsearch.xpack.core.DataTiersFeatureSetUsage;
import org.elasticsearch.xpack.core.action.XPackUsageRequestBuilder;
import org.elasticsearch.xpack.core.action.XPackUsageResponse;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class DataTierIT extends ESIntegTestCase {
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
        assertThat(DataTierAllocationDecider.INDEX_ROUTING_PREFER_SETTING.get(idxSettings), equalTo(DataTier.DATA_CONTENT));

        // index should be red
        assertThat(client().admin().cluster().prepareHealth(index).get().getIndices().get(index).getStatus(),
            equalTo(ClusterHealthStatus.RED));

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

    public void testOverrideDefaultAllocation() {
        startWarmOnlyNode();
        startColdOnlyNode();
        ensureGreen();

        client().admin().indices().prepareCreate(index)
            .setWaitForActiveShards(0)
            .setSettings(Settings.builder()
                .put(DataTierAllocationDecider.INDEX_ROUTING_PREFER, DataTier.DATA_WARM))
            .get();

        Settings idxSettings = client().admin().indices().prepareGetIndex().addIndices(index).get().getSettings().get(index);
        assertThat(idxSettings.get(DataTierAllocationDecider.INDEX_ROUTING_PREFER), equalTo(DataTier.DATA_WARM));

        // index should be yellow
        logger.info("--> waiting for {} to be yellow", index);
        ensureYellow(index);
    }

    public void testRequestSettingOverridesAllocation() {
        startWarmOnlyNode();
        startColdOnlyNode();
        ensureGreen();

        client().admin().indices().prepareCreate(index)
            .setWaitForActiveShards(0)
            .setSettings(Settings.builder()
                .putNull(DataTierAllocationDecider.INDEX_ROUTING_PREFER))
            .get();

        Settings idxSettings = client().admin().indices().prepareGetIndex().addIndices(index).get().getSettings().get(index);
        assertThat(DataTierAllocationDecider.INDEX_ROUTING_PREFER_SETTING.get(idxSettings), equalTo(""));
        // Even the key shouldn't exist if it has been nulled out
        assertFalse(idxSettings.keySet().toString(), idxSettings.keySet().contains(DataTierAllocationDecider.INDEX_ROUTING_PREFER));

        // index should be yellow
        logger.info("--> waiting for {} to be yellow", index);
        ensureYellow(index);

        client().admin().indices().prepareDelete(index).get();

        // Now test it overriding the "require" setting, in which case the preference should be skipped
        client().admin().indices().prepareCreate(index)
            .setWaitForActiveShards(0)
            .setSettings(Settings.builder()
                .put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + ".box", "cold"))
            .get();

        idxSettings = client().admin().indices().prepareGetIndex().addIndices(index).get().getSettings().get(index);
        assertThat(DataTierAllocationDecider.INDEX_ROUTING_PREFER_SETTING.get(idxSettings), equalTo(""));
        // The key should not be put in place since it was overridden
        assertFalse(idxSettings.keySet().contains(DataTierAllocationDecider.INDEX_ROUTING_PREFER));
        assertThat(idxSettings.get(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + ".box"), equalTo("cold"));

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

        client().admin().indices().prepareCreate(index)
            .setWaitForActiveShards(0)
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(DataTierAllocationDecider.INDEX_ROUTING_PREFER, "data_warm"))
            .get();

        client().admin().indices().prepareAddBlock(IndexMetadata.APIBlock.READ_ONLY, index).get();
        client().admin().indices().prepareResizeIndex(index, index + "-shrunk")
            .setResizeType(ResizeType.SHRINK)
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()).get();

        ensureGreen(index + "-shrunk");

        Settings idxSettings = client().admin().indices().prepareGetIndex().addIndices(index + "-shrunk")
            .get().getSettings().get(index + "-shrunk");
        // It should inherit the setting of its originator
        assertThat(DataTierAllocationDecider.INDEX_ROUTING_PREFER_SETTING.get(idxSettings), equalTo(DataTier.DATA_WARM));

        // Required or else the test cleanup fails because it can't delete the indices
        client().admin().indices().prepareUpdateSettings(index, index + "-shrunk")
            .setSettings(Settings.builder()
                .put("index.blocks.read_only", false))
            .get();
    }

    public void testTemplateOverridesDefaults() {
        startWarmOnlyNode();

        Template t = new Template(Settings.builder()
            .put(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX + ".box", "warm")
            .build(), null, null);
        ComposableIndexTemplate ct = new ComposableIndexTemplate.Builder()
            .indexPatterns(Collections.singletonList(index))
            .template(t).build();
        client().execute(PutComposableIndexTemplateAction.INSTANCE,
            new PutComposableIndexTemplateAction.Request("template").indexTemplate(ct)).actionGet();

        client().admin().indices().prepareCreate(index).setWaitForActiveShards(0).get();

        Settings idxSettings = client().admin().indices().prepareGetIndex().addIndices(index).get().getSettings().get(index);
        assertThat(idxSettings.keySet().contains(DataTierAllocationDecider.INDEX_ROUTING_PREFER), equalTo(false));

        // index should be yellow
        ensureYellow(index);

        client().admin().indices().prepareDelete(index).get();

        t = new Template(Settings.builder()
            .putNull(DataTierAllocationDecider.INDEX_ROUTING_PREFER)
            .build(), null, null);
        ct = new ComposableIndexTemplate.Builder().indexPatterns(Collections.singletonList(index))
                 .template(t).build();
        client().execute(PutComposableIndexTemplateAction.INSTANCE,
            new PutComposableIndexTemplateAction.Request("template").indexTemplate(ct)).actionGet();

        client().admin().indices().prepareCreate(index).setWaitForActiveShards(0).get();

        idxSettings = client().admin().indices().prepareGetIndex().addIndices(index).get().getSettings().get(index);
        assertThat(idxSettings.keySet().contains(DataTierAllocationDecider.INDEX_ROUTING_PREFER), equalTo(false));

        ensureYellow(index);
    }

    public void testDataTierTelemetry() {
        startContentOnlyNode();
        startContentOnlyNode();
        startHotOnlyNode();

        client().admin().indices().prepareCreate(index)
            .setSettings(Settings.builder()
                .put(DataTierAllocationDecider.INDEX_ROUTING_PREFER, "data_hot")
                .put("index.number_of_shards", 2)
                .put("index.number_of_replicas", 0))
            .setWaitForActiveShards(0)
            .get();

        client().admin().indices().prepareCreate(index + "2")
            .setSettings(Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1))
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

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> createIndex(index, Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put(DataTierAllocationDecider.INDEX_ROUTING_PREFER, DataTier.DATA_FROZEN)
                .build()));
        assertThat(e.getMessage(), equalTo("[data_frozen] tier can only be used for partial searchable snapshots"));

        String initialTier = randomFrom(DataTier.DATA_HOT, DataTier.DATA_WARM, DataTier.DATA_COLD);
        createIndex(index, Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put(DataTierAllocationDecider.INDEX_ROUTING_PREFER, initialTier)
            .build());

        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> updatePreference(DataTier.DATA_FROZEN));
        assertThat(e2.getMessage(), equalTo("[data_frozen] tier can only be used for partial searchable snapshots"));

        updatePreference(randomValueOtherThan(initialTier, () -> randomFrom(DataTier.DATA_HOT, DataTier.DATA_WARM, DataTier.DATA_COLD)));
    }

    private void updatePreference(String tier) {
        client().admin().indices().updateSettings(new UpdateSettingsRequest(index)
            .settings(Map.of(DataTierAllocationDecider.INDEX_ROUTING_PREFER, tier))).actionGet();
    }

    private DataTiersFeatureSetUsage getUsage() {
        XPackUsageResponse usages = new XPackUsageRequestBuilder(client()).execute().actionGet();
        return usages.getUsages().stream()
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
        Settings nodeSettings = Settings.builder()
            .putList("node.roles", Arrays.asList("master", "data_warm", "ingest"))
            .put("node.attr.box", "warm")
            .build();
        internalCluster().startNode(nodeSettings);
    }

    public void startColdOnlyNode() {
        Settings nodeSettings = Settings.builder()
            .putList("node.roles", Arrays.asList("master", "data_cold", "ingest"))
            .put("node.attr.box", "cold")
            .build();
        internalCluster().startNode(nodeSettings);
    }

    public void startFrozenOnlyNode() {
        Settings nodeSettings = Settings.builder()
            .putList("node.roles", Arrays.asList("master", "data_frozen", "ingest"))
            .put("node.attr.box", "frozen")
            .build();
        internalCluster().startNode(nodeSettings);
    }
}
