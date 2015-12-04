/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;

import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.watcher.WatcherModule;
import org.elasticsearch.watcher.test.AbstractWatcherIntegrationTestCase;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

/**
 */
@ESIntegTestCase.ClusterScope(scope = TEST, numClientNodes = 0, transportClientRatio = 0, randomDynamicTemplates = false, numDataNodes = 1)
public class WatcherIndexTemplateRegistryTests extends AbstractWatcherIntegrationTestCase {
    public void testTemplates() throws Exception {
        assertAcked(
                client().admin().cluster().prepareUpdateSettings()
                        .setTransientSettings(Settings.builder()
                                .put("watcher.history.index.key1", "value"))
                        .get()
        );

        assertBusy(new Runnable() {
            @Override
            public void run() {
                GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates(WatcherModule.HISTORY_TEMPLATE_NAME).get();
                assertThat(response.getIndexTemplates().size(), equalTo(1));
                // setting from the file on the classpath:
                assertThat(response.getIndexTemplates().get(0).getSettings().getAsBoolean("index.mapper.dynamic", null), is(false));
                // additional setting defined in the node settings:
                assertThat(response.getIndexTemplates().get(0).getSettings().get("index.key1"), equalTo("value"));
            }
        });

        // Now delete the index template and verify the index template gets added back:
        assertAcked(client().admin().indices().prepareDeleteTemplate(WatcherModule.HISTORY_TEMPLATE_NAME).get());

        assertBusy(new Runnable() {
            @Override
            public void run() {
                GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates(WatcherModule.HISTORY_TEMPLATE_NAME).get();
                assertThat(response.getIndexTemplates().size(), equalTo(1));
                // setting from the file on the classpath:
                assertThat(response.getIndexTemplates().get(0).getSettings().getAsBoolean("index.mapper.dynamic", null), is(false));
                // additional setting defined in the node settings:
                assertThat(response.getIndexTemplates().get(0).getSettings().get("index.key1"), equalTo("value"));
            }
        });
    }
}
