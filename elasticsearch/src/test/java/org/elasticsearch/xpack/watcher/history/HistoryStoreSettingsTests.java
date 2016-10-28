/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.history;

import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.watcher.support.WatcherIndexTemplateRegistry;
import org.elasticsearch.xpack.watcher.test.AbstractWatcherIntegrationTestCase;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

@TestLogging("org.elasticsearch.cluster:DEBUG,org.elasticsearch.action.admin.cluster.settings:DEBUG,org.elasticsearch.xpack.watcher:DEBUG")
@ESIntegTestCase.ClusterScope(scope = TEST, numClientNodes = 0, transportClientRatio = 0, randomDynamicTemplates = false,
        supportsDedicatedMasters = false, numDataNodes = 1)
public class HistoryStoreSettingsTests extends AbstractWatcherIntegrationTestCase {

    public void testChangeSettings() throws Exception {
        GetIndexTemplatesResponse response = client().admin().indices()
                .prepareGetTemplates(WatcherIndexTemplateRegistry.HISTORY_TEMPLATE_NAME).get();
        assertThat(response.getIndexTemplates().get(0).getSettings().get("index.number_of_shards"), equalTo("1"));
        // this isn't defined in the template, so we rely on ES's default, which is zero
        assertThat(response.getIndexTemplates().get(0).getSettings().get("index.number_of_replicas"), nullValue());
        // this isn't defined in the template, so we rely on ES's default, which is 1s
        assertThat(response.getIndexTemplates().get(0).getSettings().get("index.refresh_interval"), nullValue());
        assertAcked(
                client().admin().cluster().prepareUpdateSettings()
                        .setTransientSettings(Settings.builder()
                                .put("xpack.watcher.history.index.number_of_shards", "2")
                                .put("xpack.watcher.history.index.number_of_replicas", "2")
                                .put("xpack.watcher.history.index.refresh_interval", "5m"))
                        .get()
        );

        // use assertBusy(...) because we update the index template in an async manner
        assertBusy(new Runnable() {
            @Override
            public void run() {
                GetIndexTemplatesResponse response = client().admin().indices()
                        .prepareGetTemplates(WatcherIndexTemplateRegistry.HISTORY_TEMPLATE_NAME).get();
                assertThat(response.getIndexTemplates().get(0).getSettings().get("index.number_of_shards"), equalTo("2"));
                assertThat(response.getIndexTemplates().get(0).getSettings().get("index.number_of_replicas"), equalTo("2"));
                assertThat(response.getIndexTemplates().get(0).getSettings().get("index.refresh_interval"), equalTo("5m"));
            }
        });
    }

    public void testChangeSettingsIgnoringForbiddenSetting() throws Exception {
        GetIndexTemplatesResponse response = client().admin().indices()
                .prepareGetTemplates(WatcherIndexTemplateRegistry.HISTORY_TEMPLATE_NAME).get();
        assertThat(response.getIndexTemplates().get(0).getSettings().get("index.number_of_shards"), equalTo("1"));
        assertThat(response.getIndexTemplates().get(0).getSettings().getAsBoolean("index.mapper.dynamic", null), is(false));

        assertAcked(
                client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder()
                        .put("xpack.watcher.history.index.number_of_shards", "2")
                        .put("xpack.watcher.history.index.mapper.dynamic", true)) // forbidden setting, should not get updated
                .get()
        );

        // use assertBusy(...) because we update the index template in an async manner
        assertBusy(new Runnable() {
            @Override
            public void run() {
                GetIndexTemplatesResponse response = client().admin().indices()
                        .prepareGetTemplates(WatcherIndexTemplateRegistry.HISTORY_TEMPLATE_NAME).get();
                assertThat(response.getIndexTemplates().get(0).getSettings().get("index.number_of_shards"), equalTo("2"));
                assertThat(response.getIndexTemplates().get(0).getSettings().getAsBoolean("index.mapper.dynamic", null), is(false));
            }
        });
    }

}
