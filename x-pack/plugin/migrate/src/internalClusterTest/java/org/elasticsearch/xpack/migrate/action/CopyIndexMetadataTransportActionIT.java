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

public class CopyIndexMetadataTransportActionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
//        return List.of();
        return List.of(MigratePlugin.class, ReindexPlugin.class, MockTransportService.TestPlugin.class, DataStreamsPlugin.class);
    }

    public void testCreationDate() throws Exception {
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        indicesAdmin().create(new CreateIndexRequest(sourceIndex)).get();

        // so creation date is different
        safeSleep(3);

        var destIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        indicesAdmin().create(new CreateIndexRequest(destIndex)).get();


        // verify source and dest date are actually different before copying
        var settingsResponse = indicesAdmin().getSettings(new GetSettingsRequest().indices(sourceIndex, destIndex)).actionGet();
        var indexToSettings = settingsResponse.getIndexToSettings();
        var sourceDate = indexToSettings.get(sourceIndex).getAsLong(IndexMetadata.SETTING_CREATION_DATE, 0L);
        {
            var destDate = indexToSettings.get(destIndex).getAsLong(IndexMetadata.SETTING_CREATION_DATE, 0L);
            assertTrue(sourceDate > 0);
            assertTrue(destDate > 0);
            assertNotEquals(sourceDate, destDate);
        }

        assertAcked(
            client().execute(CopyIndexMetadataAction.INSTANCE, new CopyIndexMetadataAction.Request(TEST_REQUEST_TIMEOUT, sourceIndex, destIndex))
        );

        var destDate = indicesAdmin().getSettings(new GetSettingsRequest().indices(sourceIndex, destIndex)).actionGet()
            .getIndexToSettings()
            .get(destIndex)
            .getAsLong(IndexMetadata.SETTING_CREATION_DATE, 0L);
        assertEquals(sourceDate, destDate);
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
        var settingsResponse = indicesAdmin().getSettings(new GetSettingsRequest().indices(sourceIndex, destIndex)).actionGet();
        var destSettings = settingsResponse.getIndexToSettings().get(destIndex);
        var sourceSettings = settingsResponse.getIndexToSettings().get(sourceIndex);

        // sanity check that source settings were added
        assertEquals(true, sourceSettings.getAsBoolean("index.soft_deletes.enabled", false));
        assertEquals(numShards, Integer.parseInt(destSettings.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS)));

        // check that old setting was not added to index
        assertNull(destSettings.get("index.soft_deletes.enabled"));
        assertEquals(numShards, Integer.parseInt(destSettings.get(IndexMetadata.SETTING_NUMBER_OF_SHARDS)));
    }


}
