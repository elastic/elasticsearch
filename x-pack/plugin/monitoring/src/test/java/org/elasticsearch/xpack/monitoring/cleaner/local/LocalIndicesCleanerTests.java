/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.cleaner.local;

import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xpack.monitoring.cleaner.AbstractIndicesCleanerTestCase;
import org.elasticsearch.xpack.monitoring.exporter.local.LocalExporter;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class LocalIndicesCleanerTests extends AbstractIndicesCleanerTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), InternalSettingsPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("xpack.monitoring.exporters._local.type", LocalExporter.TYPE)
            .build();
    }

    @Override
    protected void createIndex(String name, ZonedDateTime creationDate) {
        long creationMillis = creationDate.toInstant().toEpochMilli();
        assertAcked(prepareCreate(name).setSettings(Settings.builder().put(IndexMetadata.SETTING_CREATION_DATE, creationMillis).build()));
    }

    @Override
    protected void assertIndicesCount(int count) throws Exception {
        assertBusy(() -> {
            // we set ignore_unavailable to true for this request as the monitoring index gets deleted concurrently with this assertion
            // in some cases. When the plugin security is enabled, it expands wildcards to the existing index, which then gets deleted,
            // so when es core gets the request with the explicit index name, it throws an index not found exception as that index
            // doesn't exist anymore. If we ignore unavailable instead no error will be thrown.
            GetSettingsResponse getSettingsResponse = client().admin()
                .indices()
                .prepareGetSettings()
                .addIndices(".monitoring-*")
                .setIndicesOptions(IndicesOptions.fromOptions(true, true, true, true, true))
                .get();
            Iterator<String> indices = getSettingsResponse.getIndexToSettings().keySet().iterator();
            List<String> collectedIndices = new ArrayList<>();
            while (indices.hasNext()) {
                String next = indices.next();
                collectedIndices.add(next);
            }
            assertThat(getSettingsResponse.getIndexToSettings().size(), equalTo(count));
        });
    }
}
