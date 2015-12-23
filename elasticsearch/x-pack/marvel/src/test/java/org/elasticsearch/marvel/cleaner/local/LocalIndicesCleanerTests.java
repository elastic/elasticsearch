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
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.cleaner.AbstractIndicesCleanerTestCase;
import org.joda.time.DateTime;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class LocalIndicesCleanerTests extends AbstractIndicesCleanerTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("marvel.agent.exporters._local.type", LocalExporter.TYPE)
                .build();
    }

    @Override
    protected void createIndex(String name, DateTime creationDate) {
        assertAcked(prepareCreate(name)
                .setSettings(settingsBuilder().put(IndexMetaData.SETTING_CREATION_DATE, creationDate.getMillis()).build()));
        ensureYellow(name);
    }

    @Override
    protected void assertIndicesCount(int count) throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                try {
                    assertThat(client().admin().indices().prepareGetSettings(MarvelSettings.MARVEL_INDICES_PREFIX + "*").get().getIndexToSettings().size(),
                            equalTo(count));
                } catch (IndexNotFoundException e) {
                    if (shieldEnabled) {
                        assertThat(0, equalTo(count));
                    } else {
                        throw e;
                    }
                }
            }
        });
    }
}
