/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;

import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.watcher.history.HistoryStore;
import org.elasticsearch.watcher.support.init.proxy.ClientProxy;
import org.junit.Test;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.SUITE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

/**
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = SUITE, numClientNodes = 0, transportClientRatio = 0, randomDynamicTemplates = false, numDataNodes = 1)
public class TemplateUtilsTests extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, false)
                .build();
    }

    @Override
    protected Settings transportClientSettings() {
        return Settings.builder()
                .put(super.transportClientSettings())
                .put(PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, false)
                .build();
    }

    @Test
    public void testPutTemplate() throws Exception {
        TemplateUtils templateUtils = new TemplateUtils(Settings.EMPTY, ClientProxy.of(client()));

        Settings.Builder options = Settings.builder();
        options.put("key", "value");
        templateUtils.putTemplate(HistoryStore.INDEX_TEMPLATE_NAME, options.build());

        GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates(HistoryStore.INDEX_TEMPLATE_NAME).get();
        // setting in the file on the classpath:
        assertThat(response.getIndexTemplates().get(0).getSettings().getAsBoolean("index.mapper.dynamic", null), is(false));
        // additional setting:
        assertThat(response.getIndexTemplates().get(0).getSettings().get("index.key"), equalTo("value"));
    }

}
