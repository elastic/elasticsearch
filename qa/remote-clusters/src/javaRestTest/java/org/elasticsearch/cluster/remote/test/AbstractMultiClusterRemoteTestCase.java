/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.remote.test;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

public abstract class AbstractMultiClusterRemoteTestCase extends ESRestTestCase {

    // Defer to RemoteClusterTestCluster so credentials are defined once and propagated
    // into the container's user provisioning script via env vars.
    private static final String USER = RemoteClusterTestCluster.USER;
    private static final String PASS = RemoteClusterTestCluster.PASS;

    @ClassRule
    public static final RemoteClusterTestCluster cluster = new RemoteClusterTestCluster();

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    private static RestClient cluster1Client;
    private static RestClient cluster2Client;
    private static boolean initialized = false;

    @Override
    protected String getTestRestCluster() {
        return cluster.getCluster1HttpAddress();
    }

    @Before
    public void initClientsAndConfigureClusters() throws Exception {
        if (initialized) {
            return;
        }

        cluster1Client = buildClient(cluster.getCluster1HttpAddress());
        cluster2Client = buildClient(cluster.getCluster2HttpAddress());

        Consumer<Request> waitForYellowRequest = request -> {
            request.addParameter("wait_for_status", "yellow");
            request.addParameter("wait_for_nodes", "1");
        };
        ensureHealth(cluster1Client, waitForYellowRequest);
        ensureHealth(cluster2Client, waitForYellowRequest);

        initialized = true;
    }

    @AfterClass
    public static void destroyClients() throws IOException {
        initialized = false;
        try {
            IOUtils.close(cluster1Client, cluster2Client);
        } finally {
            cluster1Client = null;
            cluster2Client = null;
        }
    }

    protected static RestClient cluster1Client() {
        return cluster1Client;
    }

    protected static RestClient cluster2Client() {
        return cluster2Client;
    }

    private RestClient buildClient(final String url) throws IOException {
        int portSeparator = url.lastIndexOf(':');
        HttpHost httpHost = new HttpHost(
            url.substring(0, portSeparator),
            Integer.parseInt(url.substring(portSeparator + 1)),
            getProtocol()
        );
        return buildClient(restAdminSettings(), new HttpHost[] { httpHost });
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
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .put(ESRestTestCase.CERTIFICATE_AUTHORITIES, trustedCertFile)
            .build();
    }

    @Override
    protected String getProtocol() {
        return "https";
    }
}
