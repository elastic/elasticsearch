/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.jdbc.framework;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.sql.jdbc.util.IOUtils;
import org.elasticsearch.xpack.sql.net.client.SuppressForbidden;
import org.elasticsearch.xpack.sql.util.StringUtils;
import org.junit.rules.ExternalResource;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;

import static java.util.Collections.emptySet;
import static org.apache.lucene.util.LuceneTestCase.createTempDir;
import static org.apache.lucene.util.LuceneTestCase.random;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomLong;


/**
 * Hack to run an {@link InternalTestCluster} if this is being run
 * in an environment without {@code tests.rest.cluster} set for easier
 * debugging. Note that this doesn't work in the security manager is
 * enabled.
 */
public class LocalEsCluster extends ExternalResource implements CheckedSupplier<Connection, SQLException> {

    private InternalTestCluster internalTestCluster;
    private RestClient client;
    private String serverAddress = StringUtils.EMPTY;

    @Override
    @SuppressForbidden(reason = "it is a hack anyway")
    protected void before() throws Throwable {
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
        internalTestCluster = new InternalTestCluster(seed, createTempDir(), false, true, 1, 1, name, config, 0, randomBoolean(), "", emptySet(), Function.identity());
        internalTestCluster.beforeTest(random(), 0);
        internalTestCluster.ensureAtLeastNumDataNodes(1);
        InetSocketAddress httpBound = internalTestCluster.httpAddresses()[0];
        String http = httpBound.getHostString() + ":" + httpBound.getPort();
        try {
            System.setProperty("tests.rest.cluster", http);
        } catch (SecurityException e) {
            throw new RuntimeException("Failed to set system property required for tests. Security manager must be disabled to use this hack.", e);
        }

        client = TestUtils.restClient(httpBound.getAddress());
        // load data
        TestUtils.loadDatasetInEs(client);

        serverAddress = httpBound.getAddress().getHostAddress();
    }

    @Override
    protected void after() {
        serverAddress = StringUtils.EMPTY;
        if (internalTestCluster == null) {
            return;
        }
        IOUtils.close(client);
        IOUtils.close(internalTestCluster);
    }

    public RestClient client() {
        return client;
    }

    public String address() {
        return serverAddress;
    }

    @Override
    public Connection get() throws SQLException {
        return DriverManager.getConnection("jdbc:es://" + serverAddress);
    }
}