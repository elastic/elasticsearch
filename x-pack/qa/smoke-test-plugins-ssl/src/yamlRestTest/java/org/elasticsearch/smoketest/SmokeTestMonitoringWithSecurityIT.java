/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.smoketest;

import io.netty.util.ThreadDeathWatcher;
import io.netty.util.concurrent.GlobalEventExecutor;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.http.HttpInfo;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackUsageRequestBuilder;
import org.elasticsearch.xpack.core.action.XPackUsageResponse;
import org.elasticsearch.xpack.core.monitoring.MonitoringFeatureSetUsage;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.ExternalResource;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest.Metric;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

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

    /**
     * A JUnit class level rule that runs after the AfterClass method in {@link ESIntegTestCase},
     * which stops the cluster. After the cluster is stopped, there are a few netty threads that
     * can linger, so we wait for them to finish otherwise these lingering threads can intermittently
     * trigger the thread leak detector
     */
    @ClassRule
    public static final ExternalResource STOP_NETTY_RESOURCE = new ExternalResource() {
        @Override
        protected void after() {
            try {
                GlobalEventExecutor.INSTANCE.awaitInactivity(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IllegalStateException e) {
                if (e.getMessage().equals("thread was not started") == false) {
                    throw e;
                }
                // ignore since the thread was never started
            }

            try {
                ThreadDeathWatcher.awaitInactivity(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    };

    private static final String USER = "test_user";
    private static final String PASS = "x-pack-test-password";
    private static final String MONITORING_PATTERN = ".monitoring-*";

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singletonList(XPackPlugin.class);
    }

    @Override
    protected Settings externalClusterClientSettings() {
        return Settings.builder()
            .put(SecurityField.USER_SETTING.getKey(), USER + ":" + PASS)
            .put(NetworkModule.TRANSPORT_TYPE_KEY, SecurityField.NAME4)
            .build();
    }

    @Before
    public void enableExporter() throws Exception {
        Settings exporterSettings = Settings.builder()
            .put("xpack.monitoring.collection.enabled", true)
            .put("xpack.monitoring.exporters._http.enabled", true)
            .put("xpack.monitoring.exporters._http.type", "http")
            .put("xpack.monitoring.exporters._http.host", "https://" + randomNodeHttpAddress())
            .put("xpack.monitoring.exporters._http.auth.username", "monitoring_agent")
            .put("xpack.monitoring.exporters._http.auth.password", "x-pack-test-password")
            .put("xpack.monitoring.exporters._http.ssl.verification_mode", "full")
            .put("xpack.monitoring.exporters._http.ssl.certificate_authorities", "testnode.crt")
            .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(exporterSettings));
    }

    @After
    public void disableExporter() {
        Settings exporterSettings = Settings.builder()
            .putNull("xpack.monitoring.collection.enabled")
            .putNull("xpack.monitoring.exporters._http.enabled")
            .putNull("xpack.monitoring.exporters._http.type")
            .putNull("xpack.monitoring.exporters._http.host")
            .putNull("xpack.monitoring.exporters._http.auth.username")
            .putNull("xpack.monitoring.exporters._http.auth.password")
            .putNull("xpack.monitoring.exporters._http.ssl.verification_mode")
            .putNull("xpack.monitoring.exporters._http.ssl.certificate_authorities")
            .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(exporterSettings));
    }

    private boolean getMonitoringUsageExportersDefined() throws Exception {
        final XPackUsageResponse usageResponse = new XPackUsageRequestBuilder(client()).execute().get();
        final Optional<MonitoringFeatureSetUsage> monitoringUsage = usageResponse.getUsages()
            .stream()
            .filter(usage -> usage instanceof MonitoringFeatureSetUsage)
            .map(usage -> (MonitoringFeatureSetUsage) usage)
            .findFirst();

        assertThat("Monitoring feature set does not exist", monitoringUsage.isPresent(), is(true));

        return monitoringUsage.get().getExporters().isEmpty() == false;
    }

    public void testHTTPExporterWithSSL() throws Exception {
        // Ensures that the exporter is actually on
        assertBusy(() -> assertThat("[_http] exporter is not defined", getMonitoringUsageExportersDefined(), is(true)));

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
        ensureYellowAndNoInitializingShards(MONITORING_PATTERN);

        // Checks that the HTTP exporter has successfully exported some data
        assertBusy(() -> {
            try {
                assertThat(client().prepareSearch(MONITORING_PATTERN).setSize(0).get().getHits().getTotalHits().value, greaterThan(0L));
            } catch (Exception e) {
                fail("exception when checking for monitoring documents: " + e.getMessage());
            }
        });
    }

    private String randomNodeHttpAddress() {
        List<NodeInfo> nodes = client().admin().cluster().prepareNodesInfo().clear().addMetric(Metric.HTTP.metricName()).get().getNodes();
        assertThat(nodes.size(), greaterThan(0));

        InetSocketAddress[] httpAddresses = new InetSocketAddress[nodes.size()];
        for (int i = 0; i < nodes.size(); i++) {
            httpAddresses[i] = nodes.get(i).getInfo(HttpInfo.class).address().publishAddress().address();
        }
        return NetworkAddress.format(randomFrom(httpAddresses));
    }

}
