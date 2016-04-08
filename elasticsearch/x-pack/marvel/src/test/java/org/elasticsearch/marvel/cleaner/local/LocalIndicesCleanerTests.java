/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.cleaner.local;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.marvel.agent.exporter.local.LocalExporter;
import org.elasticsearch.marvel.cleaner.AbstractIndicesCleanerTestCase;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class LocalIndicesCleanerTests extends AbstractIndicesCleanerTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(InternalSettingsPlugin.class);
        return plugins;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("xpack.monitoring.agent.exporters._local.type", LocalExporter.TYPE)
                .build();
    }

    @Override
    protected void createIndex(String name, DateTime creationDate) {
        assertAcked(prepareCreate(name)
                .setSettings(Settings.builder().put(IndexMetaData.SETTING_CREATION_DATE, creationDate.getMillis()).build()));
        ensureYellow(name);
    }

    @Override
    protected void assertIndicesCount(int count) throws Exception {
        assertBusy(() -> {
            try {
                assertThat(client().admin().indices().prepareGetSettings().get().getIndexToSettings().size(), equalTo(count));
            } catch (IndexNotFoundException e) {
                if (shieldEnabled) {
                    assertThat(0, equalTo(count));
                } else {
                    throw e;
                }
            }
        });
    }
}
