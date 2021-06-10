/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.remote.test;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

public abstract class AbstractMultiClusterRemoteTestCase extends ESRestTestCase {

    private static final String USER = "x_pack_rest_user";
    private static final String PASS = "x-pack-test-password";

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    private static RestHighLevelClient cluster1Client;
    private static RestHighLevelClient cluster2Client;
    private static boolean initialized = false;


    @Override
    protected String getTestRestCluster() {
        return "localhost:" + getProperty("test.fixtures.elasticsearch-" + getDistribution() + "-1.tcp.9200");
    }

    @Before
    public void initClientsAndConfigureClusters() throws Exception {
        if (initialized) {
            return;
        }

        cluster1Client = buildClient("localhost:" + getProperty("test.fixtures.elasticsearch-" + getDistribution() + "-1.tcp.9200"));
        cluster2Client = buildClient("localhost:" + getProperty("test.fixtures.elasticsearch-" + getDistribution() + "-2.tcp.9200"));

        cluster1Client().cluster().health(new ClusterHealthRequest().waitForNodes("1").waitForYellowStatus(), RequestOptions.DEFAULT);
        cluster2Client().cluster().health(new ClusterHealthRequest().waitForNodes("1").waitForYellowStatus(), RequestOptions.DEFAULT);

        initialized = true;
    }

    protected String getDistribution() {
        String distribution = System.getProperty("tests.distribution", "default");
        if (distribution.equals("oss") == false && distribution.equals("default") == false) {
            throw new IllegalArgumentException("supported values for tests.distribution are oss or default but it was " + distribution);
        }
        return distribution;
    }

    @AfterClass
    public static void destroyClients() throws IOException {
        try {
            IOUtils.close(cluster1Client, cluster2Client);
        } finally {
            cluster1Client = null;
            cluster2Client = null;
        }
    }

    protected static RestHighLevelClient cluster1Client() {
        return cluster1Client;
    }

    protected static RestHighLevelClient cluster2Client() {
        return cluster2Client;
    }

    private static class HighLevelClient extends RestHighLevelClient {
        private HighLevelClient(RestClient restClient) {
            super(restClient, RestClient::close, Collections.emptyList());
        }
    }

    private RestHighLevelClient buildClient(final String url) throws IOException {
        int portSeparator = url.lastIndexOf(':');
        HttpHost httpHost = new HttpHost(url.substring(0, portSeparator),
            Integer.parseInt(url.substring(portSeparator + 1)), getProtocol());
        return new HighLevelClient(buildClient(restAdminSettings(), new HttpHost[]{httpHost}));
    }

    protected boolean isOss() {
        return getDistribution().equals("oss");
    }

    static Path trustedCertFile;

    @BeforeClass
    public static void getTrustedCert() {
        try {
            trustedCertFile = PathUtils.get(AbstractMultiClusterRemoteTestCase.class.getResource("/testnode.crt").toURI());
        } catch (URISyntaxException e) {
            throw new ElasticsearchException("exception while reading the certificate file", e);
        }
        if (Files.exists(trustedCertFile) == false) {
            throw new IllegalStateException("Certificate file [" + trustedCertFile + "] does not exist.");
        }
    }

    @AfterClass
    public static void clearTrustedCert() {
        trustedCertFile = null;
    }

    @Override
    protected Settings restClientSettings() {
        if (isOss()) {
            return super.restClientSettings();
        }
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .put(ESRestTestCase.CERTIFICATE_AUTHORITIES, trustedCertFile)
            .build();
    }

    @Override
    protected String getProtocol() {
        if (isOss()) {
            return "http";
        }
        return "https";
    }

    private String getProperty(String key) {
        String value = System.getProperty(key);
        if (value == null) {
            throw new IllegalStateException("Could not find system properties from test.fixtures. " +
                "This test expects to run with the elasticsearch.test.fixtures Gradle plugin");
        }
        return value;
    }
}
