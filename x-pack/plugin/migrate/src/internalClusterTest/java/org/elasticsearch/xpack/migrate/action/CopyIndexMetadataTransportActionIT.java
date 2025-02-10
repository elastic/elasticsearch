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
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.migrate.MigratePlugin;

import java.util.Collection;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class CopyIndexMetadataTransportActionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            MigratePlugin.class,
            ReindexPlugin.class,
            MockTransportService.TestPlugin.class,
            DataStreamsPlugin.class,
            IngestCommonPlugin.class
        );
    }

    public void testCreationDate() throws Exception {
        var sourceIndex = randomAlphaOfLength(20).toLowerCase(Locale.ROOT);
        indicesAdmin().create(new CreateIndexRequest(sourceIndex)).get();

        // so creation date is different
        safeSleep(2);

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
            client().execute(
                CopyIndexMetadataAction.INSTANCE,
                new CopyIndexMetadataAction.Request(TEST_REQUEST_TIMEOUT, sourceIndex, destIndex)
            )
        );

        var destDate = indicesAdmin().getSettings(new GetSettingsRequest().indices(sourceIndex, destIndex))
            .actionGet()
            .getIndexToSettings()
            .get(destIndex)
            .getAsLong(IndexMetadata.SETTING_CREATION_DATE, 0L);
        assertEquals(sourceDate, destDate);
    }
}
