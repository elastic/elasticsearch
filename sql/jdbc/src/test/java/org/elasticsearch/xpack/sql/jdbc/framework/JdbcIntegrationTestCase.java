/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.framework;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.sql.jdbc.jdbc.JdbcDriver;
import org.elasticsearch.xpack.sql.net.client.SuppressForbidden;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Collection;
import java.util.TimeZone;
import java.util.function.Function;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;

public abstract class JdbcIntegrationTestCase extends ESRestTestCase {
    static {
        // Initialize the jdbc driver
        JdbcDriver.jdbcMajorVersion();
    }

    private static InternalTestCluster internalTestCluster;

    /**
     * Hack to run an {@link InternalTestCluster} if this is being run
     * in an environment without {@code tests.rest.cluster} set for easier
     * debugging. Note that this doesn't work in the security manager is
     * enabled.
     */
    @BeforeClass
    @SuppressForbidden(reason="it is a hack anyway")
    public static void startInternalTestClusterIfNeeded() throws IOException, InterruptedException {
        if (System.getProperty("tests.rest.cluster") != null) {
            // Nothing to do, using an external Elasticsearch node.
            return;
        }
        long seed = randomLong();
        String name = InternalTestCluster.clusterName("", seed);
        NodeConfigurationSource config = new NodeConfigurationSource() {
            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                Settings.Builder builder = Settings.builder()
                        // Enable http because the tests use it
                        .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                        .put(NetworkModule.HTTP_TYPE_KEY, Netty4Plugin.NETTY_HTTP_TRANSPORT_NAME)
                        // Default the watermarks to absurdly low to prevent the tests
                        // from failing on nodes without enough disk space
                        .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1b")
                        .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1b")
                        // Mimic settings in build.gradle so we're closer to real
                        .put("xpack.security.enabled", false)
                        .put("xpack.monitoring.enabled", false)
                        .put("xpack.ml.enabled", false)
                        .put("xpack.watcher.enabled", false);
                return builder.build();
            }

            @Override
            public Path nodeConfigPath(int nodeOrdinal) {
                return null;
            }

            @Override
            public Collection<Class<? extends Plugin>> nodePlugins() {
                // Use netty4 plugin to enable rest
                return Arrays.asList(Netty4Plugin.class, XPackPlugin.class, PainlessPlugin.class);
            }
        };
        internalTestCluster = new InternalTestCluster(seed, createTempDir(), false, true, 1, 1, name, config, 0, randomBoolean(), "",
                emptySet(), Function.identity());
        internalTestCluster.beforeTest(random(), 0);
        internalTestCluster.ensureAtLeastNumDataNodes(1);
        InetSocketAddress httpBound = internalTestCluster.httpAddresses()[0];
        String http = httpBound.getHostString() + ":" + httpBound.getPort();
        try {
            System.setProperty("tests.rest.cluster", http);
        } catch (SecurityException e) {
            throw new RuntimeException(
                    "Failed to set system property required for tests. Security manager must be disabled to use this hack.", e);
        }
    }

    @AfterClass
    public static void shutDownInternalTestClusterIfNeeded() {
        if (internalTestCluster == null) {
            return;
        }
        internalTestCluster.close();
    }

    protected JdbcTemplate j;

    @Before
    public void setupJdbcTemplate() throws Exception {
        j = new JdbcTemplate(() -> DriverManager.getConnection(
                "jdbc:es://" + System.getProperty("tests.rest.cluster") + "/?time_zone=" + TimeZone.getDefault().getID()));
    }

    protected void index(String index, CheckedConsumer<XContentBuilder, IOException> body) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        body.accept(builder);
        builder.endObject();
        HttpEntity doc = new StringEntity(builder.string(), ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/" + index + "/doc/1", singletonMap("refresh", "true"), doc);
    }
}
