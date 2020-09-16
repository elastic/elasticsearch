/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.cluster.routing.allocation;

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
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class DataTierIT extends ESIntegTestCase {
    private static final String index = "myindex";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(LocalStateCompositeXPackPlugin.class);
    }

    public void testDefaultIndexAllocateToContent() {
        startWarmOnlyNode();
        startColdOnlyNode();
        ensureGreen();

        client().admin().indices().prepareCreate(index).setWaitForActiveShards(0).get();

        Settings idxSettings = client().admin().indices().prepareGetIndex().addIndices(index).get().getSettings().get(index);
        assertThat(DataTierAllocationDecider.INDEX_ROUTING_INCLUDE_SETTING.get(idxSettings), equalTo(DataTier.DATA_CONTENT));

        // index should be red
        assertThat(client().admin().cluster().prepareHealth(index).get().getIndices().get(index).getStatus(),
            equalTo(ClusterHealthStatus.RED));

        if (randomBoolean()) {
            logger.info("--> starting content node");
            startContentOnlyNode();
        } else {
            logger.info("--> starting hot node");
            startDataNode();
        }

        logger.info("--> waiting for {} to be yellow", index);
        ensureYellow(index);
    }

    public void testOverrideDefaultAllocation() {
        startWarmOnlyNode();
        startColdOnlyNode();
        ensureGreen();

        String setting = randomBoolean() ? DataTierAllocationDecider.INDEX_ROUTING_REQUIRE :
            DataTierAllocationDecider.INDEX_ROUTING_INCLUDE;

        client().admin().indices().prepareCreate(index)
            .setWaitForActiveShards(0)
            .setSettings(Settings.builder()
                .put(setting, DataTier.DATA_WARM))
            .get();

        Settings idxSettings = client().admin().indices().prepareGetIndex().addIndices(index).get().getSettings().get(index);
        assertThat(idxSettings.get(setting), equalTo(DataTier.DATA_WARM));

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
                .putNull(DataTierAllocationDecider.INDEX_ROUTING_INCLUDE))
            .get();

        Settings idxSettings = client().admin().indices().prepareGetIndex().addIndices(index).get().getSettings().get(index);
        assertThat(DataTierAllocationDecider.INDEX_ROUTING_INCLUDE_SETTING.get(idxSettings), equalTo(""));
        // Even the key shouldn't exist if it has been nulled out
        assertFalse(idxSettings.keySet().toString(), idxSettings.keySet().contains(DataTierAllocationDecider.INDEX_ROUTING_INCLUDE));

        // index should be yellow
        logger.info("--> waiting for {} to be yellow", index);
        ensureYellow(index);

        client().admin().indices().prepareDelete(index).get();

        // Now test it overriding the "require" setting, in which case the include should be skipped
        client().admin().indices().prepareCreate(index)
            .setWaitForActiveShards(0)
            .setSettings(Settings.builder()
                .put(DataTierAllocationDecider.INDEX_ROUTING_REQUIRE, DataTier.DATA_COLD))
            .get();

        idxSettings = client().admin().indices().prepareGetIndex().addIndices(index).get().getSettings().get(index);
        assertThat(DataTierAllocationDecider.INDEX_ROUTING_INCLUDE_SETTING.get(idxSettings), equalTo(""));
        // The key should not be put in place since it was overridden
        assertFalse(idxSettings.keySet().contains(DataTierAllocationDecider.INDEX_ROUTING_INCLUDE));
        assertThat(DataTierAllocationDecider.INDEX_ROUTING_REQUIRE_SETTING.get(idxSettings), equalTo(DataTier.DATA_COLD));

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
                .put(DataTierAllocationDecider.INDEX_ROUTING_INCLUDE, "data_warm"))
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
        assertThat(DataTierAllocationDecider.INDEX_ROUTING_INCLUDE_SETTING.get(idxSettings), equalTo(DataTier.DATA_WARM));

        // Required or else the test cleanup fails because it can't delete the indices
        client().admin().indices().prepareUpdateSettings(index, index + "-shrunk")
            .setSettings(Settings.builder()
                .put("index.blocks.read_only", false))
            .get();
    }

    public void testTemplateOverridesDefaults() {
        startWarmOnlyNode();

        Template t = new Template(Settings.builder()
            .put(DataTierAllocationDecider.INDEX_ROUTING_REQUIRE, DataTier.DATA_WARM)
            .build(), null, null);
        ComposableIndexTemplate ct = new ComposableIndexTemplate(Collections.singletonList(index), t, null, null, null, null, null);
        client().execute(PutComposableIndexTemplateAction.INSTANCE,
            new PutComposableIndexTemplateAction.Request("template").indexTemplate(ct)).actionGet();

        client().admin().indices().prepareCreate(index).setWaitForActiveShards(0).get();

        Settings idxSettings = client().admin().indices().prepareGetIndex().addIndices(index).get().getSettings().get(index);
        assertThat(idxSettings.keySet().contains(DataTierAllocationDecider.INDEX_ROUTING_INCLUDE), equalTo(false));

        // index should be yellow
        ensureYellow(index);

        client().admin().indices().prepareDelete(index).get();

        t = new Template(Settings.builder()
            .putNull(DataTierAllocationDecider.INDEX_ROUTING_INCLUDE)
            .build(), null, null);
        ct = new ComposableIndexTemplate(Collections.singletonList(index), t, null, null, null, null, null);
        client().execute(PutComposableIndexTemplateAction.INSTANCE,
            new PutComposableIndexTemplateAction.Request("template").indexTemplate(ct)).actionGet();

        client().admin().indices().prepareCreate(index).setWaitForActiveShards(0).get();

        idxSettings = client().admin().indices().prepareGetIndex().addIndices(index).get().getSettings().get(index);
        assertThat(idxSettings.keySet().contains(DataTierAllocationDecider.INDEX_ROUTING_INCLUDE), equalTo(false));

        ensureYellow(index);
    }

    public void startDataNode() {
        Settings nodeSettings = Settings.builder()
            .putList("node.roles", Arrays.asList("master", "data", "ingest"))
            .build();
        internalCluster().startNode(nodeSettings);
    }

    public void startContentOnlyNode() {
        Settings nodeSettings = Settings.builder()
            .putList("node.roles", Arrays.asList("master", "data_content", "ingest"))
            .build();
        internalCluster().startNode(nodeSettings);
    }

    public void startHotOnlyNode() {
        Settings nodeSettings = Settings.builder()
            .putList("node.roles", Arrays.asList("master", "data_hot", "ingest"))
            .build();
        internalCluster().startNode(nodeSettings);
    }

    public void startWarmOnlyNode() {
        Settings nodeSettings = Settings.builder()
            .putList("node.roles", Arrays.asList("master", "data_warm", "ingest"))
            .build();
        internalCluster().startNode(nodeSettings);
    }

    public void startColdOnlyNode() {
        Settings nodeSettings = Settings.builder()
            .putList("node.roles", Arrays.asList("master", "data_cold", "ingest"))
            .build();
        internalCluster().startNode(nodeSettings);
    }

    public void startFrozenOnlyNode() {
        Settings nodeSettings = Settings.builder()
            .putList("node.roles", Arrays.asList("master", "data_frozen", "ingest"))
            .build();
        internalCluster().startNode(nodeSettings);
    }
}
