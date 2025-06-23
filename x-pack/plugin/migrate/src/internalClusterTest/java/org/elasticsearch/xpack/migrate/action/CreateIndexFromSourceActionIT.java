/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.action;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.migrate.MigratePlugin;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class CreateIndexFromSourceActionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MigratePlugin.class, ReindexPlugin.class, MockTransportService.TestPlugin.class, DataStreamsPlugin.class);
    }

    public void testOldSettingsManuallyFiltered() throws Exception {
        var numShards = randomIntBetween(1, 10);
        var staticSettings = Settings.builder()
            // setting to filter
            .put("index.soft_deletes.enabled", true)
            // good setting to keep
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .build();

        // start with a static setting
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        indicesAdmin().create(new CreateIndexRequest(sourceIndex, staticSettings)).get();

        // create from source
        var destIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        assertAcked(
            client().execute(CreateIndexFromSourceAction.INSTANCE, new CreateIndexFromSourceAction.Request(sourceIndex, destIndex))
        );

        // assert both static and dynamic settings set on dest index
        var settingsResponse = indicesAdmin().getSettings(new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(sourceIndex, destIndex))
            .actionGet();
        var destSettings = settingsResponse.getIndexToSettings().get(destIndex);
        var sourceSettings = settingsResponse.getIndexToSettings().get(sourceIndex);

        // sanity check that source settings were added
        assertEquals(true, sourceSettings.getAsBoolean("index.soft_deletes.enabled", false));
        assertEquals(numShards, Integer.parseInt(destSettings.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS)));

        // check that old setting was not added to index
        assertNull(destSettings.get("index.soft_deletes.enabled"));
        assertEquals(numShards, Integer.parseInt(destSettings.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS)));
    }

    public void testDestIndexCreated() throws Exception {
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        indicesAdmin().create(new CreateIndexRequest(sourceIndex)).get();

        // create from source
        var destIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        assertAcked(
            client().execute(CreateIndexFromSourceAction.INSTANCE, new CreateIndexFromSourceAction.Request(sourceIndex, destIndex))
        );

        try {
            indicesAdmin().getIndex(new GetIndexRequest(TEST_REQUEST_TIMEOUT).indices(destIndex)).actionGet();
        } catch (IndexNotFoundException e) {
            fail();
        }
    }

    public void testSettingsCopiedFromSource() throws Exception {
        // start with a static setting
        var numShards = randomIntBetween(1, 10);
        var staticSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards).build();
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        indicesAdmin().create(new CreateIndexRequest(sourceIndex, staticSettings)).get();

        // update with a dynamic setting
        var numReplicas = randomIntBetween(0, 10);
        var dynamicSettings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas).build();
        indicesAdmin().updateSettings(new UpdateSettingsRequest(dynamicSettings, sourceIndex)).actionGet();

        // create from source
        var destIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        assertAcked(
            client().execute(CreateIndexFromSourceAction.INSTANCE, new CreateIndexFromSourceAction.Request(sourceIndex, destIndex))
        );

        // assert both static and dynamic settings set on dest index
        var settingsResponse = indicesAdmin().getSettings(new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(destIndex)).actionGet();
        assertEquals(numReplicas, Integer.parseInt(settingsResponse.getSetting(destIndex, IndexMetadata.SETTING_NUMBER_OF_REPLICAS)));
        assertEquals(numShards, Integer.parseInt(settingsResponse.getSetting(destIndex, IndexMetadata.SETTING_NUMBER_OF_SHARDS)));
    }

    public void testMappingsCopiedFromSource() {
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        String mapping = """
            {
              "_doc":{
                "dynamic":"strict",
                "properties":{
                  "foo1":{
                    "type":"text"
                  }
                }
              }
            }
            """;
        indicesAdmin().create(new CreateIndexRequest(sourceIndex).mapping(mapping)).actionGet();

        // create from source
        var destIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        assertAcked(
            client().execute(CreateIndexFromSourceAction.INSTANCE, new CreateIndexFromSourceAction.Request(sourceIndex, destIndex))
        );

        var mappingsResponse = indicesAdmin().getMappings(new GetMappingsRequest(TEST_REQUEST_TIMEOUT).indices(sourceIndex, destIndex))
            .actionGet();
        Map<String, MappingMetadata> mappings = mappingsResponse.mappings();
        var destMappings = mappings.get(destIndex).sourceAsMap();
        var sourceMappings = mappings.get(sourceIndex).sourceAsMap();

        assertEquals(sourceMappings, destMappings);
        // sanity check specific value from dest mapping
        assertEquals("text", XContentMapValues.extractValue("properties.foo1.type", destMappings));
    }

    public void testSettingsOverridden() throws Exception {
        var numShardsSource = randomIntBetween(1, 10);
        var numReplicasSource = randomIntBetween(0, 10);
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        var sourceSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShardsSource)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicasSource)
            .build();
        indicesAdmin().create(new CreateIndexRequest(sourceIndex, sourceSettings)).get();

        boolean overrideNumShards = randomBoolean();
        Settings settingsOverride = overrideNumShards
            ? Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShardsSource + 1).build()
            : Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicasSource + 1).build();

        // create from source
        var destIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        assertAcked(
            client().execute(
                CreateIndexFromSourceAction.INSTANCE,
                new CreateIndexFromSourceAction.Request(sourceIndex, destIndex, settingsOverride, Map.of(), randomBoolean())
            )
        );

        // assert settings overridden
        int expectedShards = overrideNumShards ? numShardsSource + 1 : numShardsSource;
        int expectedReplicas = overrideNumShards ? numReplicasSource : numReplicasSource + 1;
        var settingsResponse = indicesAdmin().getSettings(new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(destIndex)).actionGet();
        assertEquals(expectedShards, Integer.parseInt(settingsResponse.getSetting(destIndex, IndexMetadata.SETTING_NUMBER_OF_SHARDS)));
        assertEquals(expectedReplicas, Integer.parseInt(settingsResponse.getSetting(destIndex, IndexMetadata.SETTING_NUMBER_OF_REPLICAS)));
    }

    public void testSettingsNullOverride() throws Exception {
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        var sourceSettings = Settings.builder()
            .put(IndexMetadata.SETTING_BLOCKS_WRITE, true)
            .put(IndexMetadata.SETTING_BLOCKS_READ, true)
            .build();
        indicesAdmin().create(new CreateIndexRequest(sourceIndex, sourceSettings)).get();

        Settings settingsOverride = Settings.builder().putNull(IndexMetadata.SETTING_BLOCKS_WRITE).build();

        // create from source
        var destIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        assertAcked(
            client().execute(
                CreateIndexFromSourceAction.INSTANCE,
                new CreateIndexFromSourceAction.Request(sourceIndex, destIndex, settingsOverride, Map.of(), false)
            )
        );

        // assert settings overridden
        var settingsResponse = indicesAdmin().getSettings(new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(destIndex)).actionGet();
        var destSettings = settingsResponse.getIndexToSettings().get(destIndex);

        // sanity check
        assertTrue(destSettings.getAsBoolean(IndexMetadata.SETTING_BLOCKS_READ, false));

        // override null removed
        assertThat(destSettings.keySet(), not(hasItem(IndexMetadata.SETTING_BLOCKS_WRITE)));
    }

    public void testRemoveIndexBlocksByDefault() throws Exception {
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);

        var sourceSettings = Settings.builder()
            .put(IndexMetadata.SETTING_BLOCKS_WRITE, randomBoolean())
            .put(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE, randomBoolean())
            .build();
        indicesAdmin().create(new CreateIndexRequest(sourceIndex, sourceSettings)).get();

        var settingsOverride = Settings.builder()
            .put(IndexMetadata.SETTING_BLOCKS_WRITE, randomBoolean())
            .put(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE, randomBoolean())
            .put(IndexMetadata.SETTING_BLOCKS_READ, randomBoolean())
            .build();

        // create from source
        var destIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);

        CreateIndexFromSourceAction.Request request = new CreateIndexFromSourceAction.Request(sourceIndex, destIndex);
        request.settingsOverride(settingsOverride);
        assertAcked(client().execute(CreateIndexFromSourceAction.INSTANCE, request));

        // assert settings overridden
        var settingsResponse = indicesAdmin().getSettings(new GetSettingsRequest(TEST_REQUEST_TIMEOUT).indices(destIndex)).actionGet();
        var destSettings = settingsResponse.getIndexToSettings().get(destIndex);

        // remove block settings override both source settings and override settings
        assertThat(destSettings.keySet(), not(hasItem(IndexMetadata.SETTING_BLOCKS_WRITE)));
        assertThat(destSettings.keySet(), not(hasItem(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE)));
        assertThat(destSettings.keySet(), not(hasItem(IndexMetadata.SETTING_BLOCKS_READ)));
    }

    public void testMappingsOverridden() {
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        String sourceMapping = """
            {
              "_doc":{
                "dynamic":"strict",
                "properties":{
                  "foo1":{
                    "type":"text"
                  },
                  "foo2":{
                    "type":"boolean"
                  }
                }
              }
            }
            """;
        indicesAdmin().create(new CreateIndexRequest(sourceIndex).mapping(sourceMapping)).actionGet();

        String mappingOverrideStr = """
            {
              "_doc":{
                "dynamic":"strict",
                "properties":{
                  "foo1":{
                    "type":"integer"
                  },
                  "foo3": {
                    "type":"keyword"
                  }
                }
              }
            }
            """;
        var mappingOverride = XContentHelper.convertToMap(JsonXContent.jsonXContent, mappingOverrideStr, false);

        // create from source
        var destIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        assertAcked(
            client().execute(
                CreateIndexFromSourceAction.INSTANCE,
                new CreateIndexFromSourceAction.Request(sourceIndex, destIndex, Settings.EMPTY, mappingOverride, randomBoolean())
            )
        );

        var mappingsResponse = indicesAdmin().getMappings(new GetMappingsRequest(TEST_REQUEST_TIMEOUT).indices(destIndex)).actionGet();
        Map<String, MappingMetadata> mappings = mappingsResponse.mappings();
        var destMappings = mappings.get(destIndex).sourceAsMap();

        String expectedMappingStr = """
            {
                "dynamic":"strict",
                "properties":{
                  "foo1":{
                    "type":"integer"
                  },
                  "foo2": {
                    "type":"boolean"
                  },
                  "foo3": {
                    "type":"keyword"
                  }
                }
            }
            """;
        var expectedMapping = XContentHelper.convertToMap(JsonXContent.jsonXContent, expectedMappingStr, false);
        assertEquals(expectedMapping, destMappings);
    }
}
