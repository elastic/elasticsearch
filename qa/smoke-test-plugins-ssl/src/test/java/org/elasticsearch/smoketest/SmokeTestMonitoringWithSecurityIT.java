/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.XPackPlugin;
import org.junit.After;
import org.junit.Before;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * This test checks that a Monitoring's HTTP exporter correctly exports to a monitoring cluster
 * protected by security with HTTPS/SSL.
 *
 * It sets up a cluster with Monitoring and Security configured with SSL. Once started,
 * an HTTP exporter is activated and it exports data locally over HTTPS/SSL. The test
 * then uses a transport client to check that the data have been correctly received and
 * indexed in the cluster.
 */
public class SmokeTestMonitoringWithSecurityIT extends ESIntegTestCase {
    private static final String USER = "test_user";
    private static final String PASS = "changeme";
    private static final String MONITORING_PATTERN = ".monitoring-*";

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singletonList(XPackPlugin.class);
    }

    @Override
    protected Settings externalClusterClientSettings() {
        return Settings.builder()
                .put(Security.USER_SETTING.getKey(), USER + ":" + PASS)
                .put(NetworkModule.TRANSPORT_TYPE_KEY, Security.NAME4).build();
    }

    @Before
    public void enableExporter() throws Exception {
        Settings exporterSettings = Settings.builder()
                .put("xpack.monitoring.exporters._http.enabled", true)
                .put("xpack.monitoring.exporters._http.host", "https://" + NetworkAddress.format(randomFrom(httpAddresses())))
                .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(exporterSettings));
    }

    @After
    public void disableExporter() {
        Settings exporterSettings = Settings.builder()
                .putNull("xpack.monitoring.exporters._http.enabled")
                .putNull("xpack.monitoring.exporters._http.host")
                .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(exporterSettings));
    }

    public void testHTTPExporterWithSSL() throws Exception {
        // Checks that the monitoring index templates have been installed
        assertBusy(() -> {
            GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates(MONITORING_PATTERN).get();
            assertThat(response.getIndexTemplates().size(), greaterThanOrEqualTo(2));
        });

        // Waits for monitoring indices to be created
        assertBusy(() -> {
            try {
                assertThat(client().admin().indices().prepareExists(MONITORING_PATTERN).get().isExists(), equalTo(true));
            } catch (Exception e) {
                fail("exception when checking for monitoring documents: " + e.getMessage());
            }
        });

        // Waits for indices to be ready
        ensureYellow(MONITORING_PATTERN);

        // Checks that the HTTP exporter has successfully exported some data
        assertBusy(() -> {
            try {
                assertThat(client().prepareSearch(MONITORING_PATTERN).setSize(0).get().getHits().getTotalHits(), greaterThan(0L));
            } catch (Exception e) {
                fail("exception when checking for monitoring documents: " + e.getMessage());
            }
        });
    }

    private InetSocketAddress[] httpAddresses() {
        List<NodeInfo> nodes = client().admin().cluster().prepareNodesInfo().clear().setHttp(true).get().getNodes();
        assertThat(nodes.size(), greaterThan(0));

        InetSocketAddress[] httpAddresses = new InetSocketAddress[nodes.size()];
        for (int i = 0; i < nodes.size(); i++) {
            httpAddresses[i] = nodes.get(i).getHttp().address().publishAddress().address();
        }
        return httpAddresses;
    }
}
