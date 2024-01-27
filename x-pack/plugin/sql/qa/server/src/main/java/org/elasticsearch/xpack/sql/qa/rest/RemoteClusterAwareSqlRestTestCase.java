/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.rest;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

import static org.elasticsearch.common.Strings.hasText;

public abstract class RemoteClusterAwareSqlRestTestCase extends ESRestTestCase {

    private static final long CLIENT_TIMEOUT = 40L; // upped from 10s to accommodate for max measured throughput decline

    // client used for loading data on a remote cluster only.
    private static RestClient remoteClient;

    // gradle defines when using legacy-java-rest-test
    public static final String AUTH_USER = System.getProperty("tests.rest.cluster.multi.user");
    public static final String AUTH_PASS = System.getProperty("tests.rest.cluster.multi.password");

    @BeforeClass
    public static void initRemoteClients() throws IOException {
        String crossClusterHost = System.getProperty("tests.rest.cluster.remote.host"); // gradle defined
        if (crossClusterHost != null) {
            int portSeparator = crossClusterHost.lastIndexOf(':');
            if (portSeparator < 0) {
                throw new IllegalArgumentException("Illegal cluster url [" + crossClusterHost + "]");
            }
            String host = crossClusterHost.substring(0, portSeparator);
            int port = Integer.parseInt(crossClusterHost.substring(portSeparator + 1));
            HttpHost[] remoteHttpHosts = new HttpHost[] { new HttpHost(host, port) };

            remoteClient = clientBuilder(secureRemoteClientSettings(), remoteHttpHosts);
        }
    }

    @AfterClass
    public static void closeRemoteClients() throws IOException {
        try {
            IOUtils.close(remoteClient);
        } finally {
            remoteClient = null;
        }
    }

    public static RestClient clientBuilder(Settings settings, HttpHost[] hosts) throws IOException {
        RestClientBuilder builder = RestClient.builder(hosts);
        doConfigureClient(builder, settings);

        int timeout = Math.toIntExact(timeout().millis());
        builder.setRequestConfigCallback(
            requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(timeout)
                .setConnectionRequestTimeout(timeout)
                .setSocketTimeout(timeout)
        );
        builder.setStrictDeprecationMode(true);
        return builder.build();
    }

    protected static TimeValue timeout() {
        return TimeValue.timeValueSeconds(CLIENT_TIMEOUT);
    }

    /**
     * Use this when using the {@code legacy-java-rest-test} plugin.
     * @return a client to the remote cluster if it exists, otherwise a client to the local cluster
     */
    public static RestClient defaultProvisioningClient() {
        return remoteClient == null ? client() : remoteClient;
    }

    /**
     * Override if the test data must be provisioned on a remote cluster while not using the {@code legacy-java-rest-test} plugin.
     * @return client to use for loading test data
     */
    protected RestClient provisioningClient() {
        return defaultProvisioningClient();
    }

    @Override
    protected Settings restClientSettings() {
        return secureRemoteClientSettings();
    }

    protected static Settings secureRemoteClientSettings() {
        if (hasText(AUTH_USER) && hasText(AUTH_PASS)) {
            String token = basicAuthHeaderValue(AUTH_USER, new SecureString(AUTH_PASS.toCharArray()));
            return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
        }
        return Settings.EMPTY;
    }
}
