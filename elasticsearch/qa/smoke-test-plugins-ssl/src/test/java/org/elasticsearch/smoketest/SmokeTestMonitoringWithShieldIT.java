/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.shield.Security;
import org.elasticsearch.shield.transport.netty.ShieldNettyTransport;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.XPackPlugin;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * This test checks that a Monitoring's HTTP exporter correctly exports to a monitoring cluster
 * protected by Shield with HTTPS/SSL.
 *
 * It sets up a cluster with Monitoring and Shield configured with SSL. Once started,
 * an HTTP exporter is activated and it exports data locally over HTTPS/SSL. The test
 * then uses a transport client to check that the data have been correctly received and
 * indexed in the cluster.
 */
public class SmokeTestMonitoringWithShieldIT extends ESIntegTestCase {

    private static final String USER = "test_user";
    private static final String PASS = "changeme";
    private static final String KEYSTORE_PASS = "keypass";
    
    private static final String MONITORING_PATTERN = ".monitoring-*";

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singletonList(XPackPlugin.class);
    }

    @Override
    protected Settings externalClusterClientSettings() {
        return Settings.builder()
                .put(Security.USER_SETTING.getKey(), USER + ":" + PASS)
                .put(ShieldNettyTransport.SSL_SETTING.getKey(), true)
                .put("xpack.security.ssl.keystore.path", clientKeyStore)
                .put("xpack.security.ssl.keystore.password", KEYSTORE_PASS)
                .build();
    }

    @Before
    public void enableExporter() throws Exception {
        InetSocketAddress httpAddress = randomFrom(httpAddresses());
        URI uri = new URI("https", null, httpAddress.getHostString(), httpAddress.getPort(), "/", null, null);

        Settings exporterSettings = Settings.builder()
                .put("xpack.monitoring.agent.exporters._http.enabled", true)
                .put("xpack.monitoring.agent.exporters._http.host", uri.toString())
                .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(exporterSettings));
    }

    @After
    public void disableExporter() {
        Settings exporterSettings = Settings.builder()
                .putNull("xpack.monitoring.agent.exporters._http.enabled")
                .putNull("xpack.monitoring.agent.exporters._http.host")
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
        NodeInfo[] nodes = client().admin().cluster().prepareNodesInfo().clear().setHttp(true).get().getNodes();
        assertThat(nodes.length, greaterThan(0));

        InetSocketAddress[] httpAddresses = new InetSocketAddress[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            httpAddresses[i] = ((InetSocketTransportAddress) nodes[i].getHttp().address().publishAddress()).address();
        }
        return httpAddresses;
    }

    static Path clientKeyStore;

    @BeforeClass
    public static void loadKeyStore() {
        try {
            clientKeyStore = PathUtils.get(SmokeTestMonitoringWithShieldIT.class.getResource("/test-client.jks").toURI());
        } catch (URISyntaxException e) {
            throw new ElasticsearchException("exception while reading the store", e);
        }
        if (!Files.exists(clientKeyStore)) {
            throw new IllegalStateException("Keystore file [" + clientKeyStore + "] does not exist.");
        }
    }

    @AfterClass
    public static void clearClientKeyStore() {
        clientKeyStore = null;
    }
}
