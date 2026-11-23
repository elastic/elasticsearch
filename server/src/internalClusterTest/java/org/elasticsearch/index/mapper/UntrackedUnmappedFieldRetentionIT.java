/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.admin.indices.ResizeIndexTestUtils.executeResize;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Tests that an index created before {@link IndexVersions#TRACKS_UNMAPPED_FIELD_RETENTION} never gets
 * {@link IndexSettings#RETAINS_UNMAPPED_FIELDS_SETTING} and always reads as retaining unmapped fields.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class UntrackedUnmappedFieldRetentionIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(InternalSettingsPlugin.class);
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        // Lets the test create an index on an earlier index version.
        return false;
    }

    public void testNotRecorded() {
        IndexVersion before = IndexVersionUtils.getPreviousVersion(IndexVersions.TRACKS_UNMAPPED_FIELD_RETENTION);
        Settings.Builder settings = indexSettings(1, 0).put(IndexMetadata.SETTING_VERSION_CREATED, before);
        assertAcked(prepareCreate("loose").setSettings(settings).setMapping("""
            { "dynamic": false }"""));
        assertNotRecorded("loose");

        assertAcked(prepareCreate("index").setSettings(settings).setMapping("field", "type=keyword"));
        assertNotRecorded("index");
        assertAcked(indicesAdmin().preparePutMapping("index").setSource(Map.of("dynamic", false)));
        assertNotRecorded("index");

        updateIndexSettings(Settings.builder().put("index.blocks.write", true), "index");
        ensureGreen("index");
        assertAcked(executeResize(ResizeType.CLONE, "index", "clone", indexSettings(1, 0).putNull("index.blocks.write")));
        assertNotRecorded("clone");
    }

    private void assertNotRecorded(String index) {
        Settings settings = clusterService().state().metadata().getProject().index(index).getSettings();
        assertFalse(index, settings.hasValue(IndexSettings.RETAINS_UNMAPPED_FIELDS_SETTING.getKey()));
        ensureGreen(index);
        for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
            IndexService indexService = indicesService.indexService(resolveIndex(index));
            if (indexService != null) {
                assertTrue(index, indexService.getIndexSettings().retainsUnmappedFields());
                return;
            }
        }
        throw new AssertionError("no shard of [" + index + "]");
    }
}
