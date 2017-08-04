/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlAction;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlResponse;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class SqlActionIT extends ESIntegTestCase {

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        settings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        settings.put(XPackSettings.MONITORING_ENABLED.getKey(), false);
        settings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        settings.put(XPackSettings.GRAPH_ENABLED.getKey(), false);
        settings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), false);
        settings.put(MachineLearning.AUTODETECT_PROCESS.getKey(), false);
        return settings.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(XPackPlugin.class, ReindexPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    @Override
    protected Settings transportClientSettings() {
        // Plugin should be loaded on the transport client as well
        return nodeSettings(0);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        return Arrays.asList(TestZenDiscovery.TestPlugin.class, TestSeedPlugin.class);
    }


    public void testSqlAction() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").get());
        client().prepareBulk()
                .add(new IndexRequest("test", "doc", "1").source("data", "bar", "count", 42))
                .add(new IndexRequest("test", "doc", "2").source("data", "baz", "count", 43))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        ensureYellow("test");

        boolean columnOrder = randomBoolean();
        String columns = columnOrder ? "data, count" : "count, data";
        SqlResponse response = client().prepareExecute(SqlAction.INSTANCE).query("SELECT " + columns + " FROM test ORDER BY count").get();
        assertThat(response.size(), equalTo(2L));
        assertThat(response.columns().keySet(), hasSize(2));
        assertThat(response.columns().get("data"), equalTo("text"));
        assertThat(response.columns().get("count"), equalTo("long"));

        // Check that columns were returned in the requested order
        assertThat(response.columns().keySet().iterator().next(), equalTo(columnOrder ? "data" : "count"));

        assertThat(response.rows(), hasSize(2));
        assertThat(response.rows().get(0).get("data"), equalTo("bar"));
        assertThat(response.rows().get(0).get("count"), equalTo(42L));
        assertThat(response.rows().get(1).get("data"), equalTo("baz"));
        assertThat(response.rows().get(1).get("count"), equalTo(43L));

        // Check that columns within each row were returned in the requested order
        for(Map<String, Object> row : response.rows()) {
            assertThat(row.keySet().iterator().next(), equalTo(columnOrder ? "data" : "count"));
        }
    }
}

