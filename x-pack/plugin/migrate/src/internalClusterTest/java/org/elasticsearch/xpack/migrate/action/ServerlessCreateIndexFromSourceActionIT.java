/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.migrate.MigratePlugin;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration tests for CreateIndexFromSource in serverless mode.
 * Tests that non-serverless-public settings are filtered out.
 */
public class ServerlessCreateIndexFromSourceActionIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MigratePlugin.class, ReindexPlugin.class, MockTransportService.TestPlugin.class, DataStreamsPlugin.class);
    }

    public void testServerlessFiltersNonPublicSettings() throws Exception {
        // Create a source index with both serverless-public and non-public settings
        var sourceIndex = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var sourceSettings = Settings.builder()
            // Serverless-public settings - should be kept
            .put(IndexMetadata.INDEX_HIDDEN_SETTING.getKey(), true)
            // Non-serverless-public settings - should be filtered
            .put(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "all")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(DataTier.TIER_PREFERENCE_SETTING.getKey(), DataTier.DATA_HOT)
            .build();

        indicesAdmin().create(new CreateIndexRequest(sourceIndex, sourceSettings)).get();

        // Create from source
        var destIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        assertAcked(
            client().execute(CreateIndexFromSourceAction.INSTANCE, new CreateIndexFromSourceAction.Request(sourceIndex, destIndex))
        );

        // Verify: ServerlessPublic settings copied, non-public settings filtered
        var settingsResponse = indicesAdmin().getSettings(new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(destIndex)).actionGet();
        var destSettings = settingsResponse.getIndexToSettings().get(destIndex);

        // Serverless-public settings should be present
        assertEquals("true", destSettings.get(IndexMetadata.INDEX_HIDDEN_SETTING.getKey()));

        // Non-serverless-public settings should be filtered out
        assertThat(
            "Non-serverless-public SETTING_NUMBER_OF_SHARDS setting should be filtered in serverless mode",
            destSettings.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS),
            nullValue()
        );
        assertThat(
            "Non-serverless-public SETTING_NUMBER_OF_REPLICAS setting should be filtered in serverless mode",
            destSettings.get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS),
            nullValue()
        );
        assertThat(
            "Non-serverless-public TIER_PREFERENCE_SETTING should be filtered in serverless mode",
            destSettings.get(DataTier.TIER_PREFERENCE_SETTING.getKey()),
            nullValue()
        );
    }
}

