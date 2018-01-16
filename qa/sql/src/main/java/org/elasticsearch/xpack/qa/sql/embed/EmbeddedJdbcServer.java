/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.qa.sql.embed;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.NodeConfigurationSource;
import org.elasticsearch.transport.Netty4Plugin;
import org.elasticsearch.xpack.qa.sql.jdbc.DataLoader;
import org.junit.rules.ExternalResource;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import java.util.function.Function;

import static org.apache.lucene.util.LuceneTestCase.createTempDir;
import static org.apache.lucene.util.LuceneTestCase.random;
import static org.elasticsearch.test.ESTestCase.randomLong;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Embedded JDBC server that uses the internal test cluster in the same JVM as the tests.
 */
public class EmbeddedJdbcServer extends ExternalResource {

    private InternalTestCluster internalTestCluster;
    private String jdbcUrl;
    private final Properties properties;

    public EmbeddedJdbcServer() {
        this(false);
    }

    public EmbeddedJdbcServer(boolean debug) {
        properties = new Properties();
        if (debug) {
            properties.setProperty("debug", "true");
        }
    }

    @Override
    @SuppressWarnings("resource")
    protected void before() throws Throwable {
        int numNodes = 1;
        internalTestCluster = new InternalTestCluster(randomLong(), createTempDir(), false, true, numNodes, numNodes,
                "sql_embed", new SqlNodeConfigurationSource(), 0, false, "sql_embed",
                Arrays.asList(Netty4Plugin.class, SqlEmbedPlugin.class, PainlessPlugin.class),
                Function.identity());
        internalTestCluster.beforeTest(random(), 0.5);
        Tuple<String, Integer> address =  getHttpAddress();
        jdbcUrl = "jdbc:es://" + address.v1() + ":" + address.v2();
        System.setProperty("tests.rest.cluster", address.v1() + ":" + address.v2());
    }

    private Tuple<String, Integer> getHttpAddress() {
        NodesInfoResponse nodesInfoResponse = internalTestCluster.client().admin().cluster().prepareNodesInfo().get();
        assertFalse(nodesInfoResponse.hasFailures());
        for (NodeInfo node : nodesInfoResponse.getNodes()) {
            if (node.getHttp() != null) {
                TransportAddress publishAddress = node.getHttp().address().publishAddress();
                return new Tuple<>(publishAddress.getAddress(), publishAddress.getPort());
            }
        }
        throw new IllegalStateException("No http servers found");
    }

    @Override
    protected void after() {
        try {
            internalTestCluster.afterTest();
        } catch (IOException e) {
            fail("Failed to shutdown server " + e.getMessage());
        } finally {
            internalTestCluster.close();
        }
    }

    public Connection connection(Properties props) throws SQLException {
        assertNotNull("ES JDBC Server is null - make sure ES is properly run as a @ClassRule", jdbcUrl);
        Properties p = new Properties(properties);
        p.putAll(props);
        return DriverManager.getConnection(jdbcUrl, p);
    }

    private static class SqlNodeConfigurationSource extends NodeConfigurationSource {

        @Override
        public Settings nodeSettings(int nodeOrdinal) {
            return Settings.builder()
                    .put(NetworkModule.HTTP_ENABLED.getKey(), true) //This test requires HTTP
                    .build();
        }

        @Override
        public Path nodeConfigPath(int nodeOrdinal) {
            return null;
        }
    }
}