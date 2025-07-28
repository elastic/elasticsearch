/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.eql;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.Strings.hasText;

public abstract class RemoteClusterAwareEqlRestTestCase extends ESRestTestCase {

    private static final long CLIENT_TIMEOUT = 40L; // upped from 10s to accomodate for max measured throughput decline

    // client used for loading data on a remote cluster only.
    private static RestClient remoteClient;

    @Before
    public void initRemoteClients() throws IOException {
        if (remoteClient == null) {
            String crossClusterHost = getRemoteCluster();
            if (crossClusterHost != null) {
                List<HttpHost> httpHosts = parseClusterHosts(crossClusterHost);
                remoteClient = clientBuilder(secureRemoteClientSettings(), httpHosts.toArray(new HttpHost[0]));
            }
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

    protected String getRemoteCluster() {
        return System.getProperty("tests.rest.cluster.remote.host");
    }

    protected static RestClient clientBuilder(Settings settings, HttpHost[] hosts) throws IOException {
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

    // returned client is used to load the test data, either in the local cluster (for rest/javaRestTests) or a remote one (for
    // multi-cluster). note: the client()/adminClient() will always connect to the local cluster.
    protected static RestClient provisioningClient() {
        return remoteClient == null ? client() : remoteClient;
    }

    protected Boolean ccsMinimizeRoundtrips() {
        return remoteClient == null ? null : randomBoolean();
    }

    protected static RestClient provisioningAdminClient() {
        return remoteClient == null ? adminClient() : remoteClient;
    }

    protected static void createIndex(String name, String aliases) throws IOException {
        Settings settings = Settings.EMPTY;
        Request request = new Request("PUT", "/" + name);
        String entity = "{\"settings\": " + Strings.toString(settings);
        if (aliases != null) {
            entity += ",\"aliases\": {" + aliases + "}";
        }
        entity += "}";
        if (settings.getAsBoolean(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true) == false) {
            expectSoftDeletesWarning(request, name);
        }
        request.setJsonEntity(entity);
        provisioningClient().performRequest(request);
    }

    protected static void deleteIndexWithProvisioningClient(String name) throws IOException {
        deleteIndex(provisioningClient(), name);
    }

    @Override
    protected Settings restClientSettings() {
        return secureRemoteClientSettings();
    }

    protected static Settings secureRemoteClientSettings() {
        String user = System.getProperty("tests.rest.cluster.remote.user", "test_user"); // gradle defined
        String pass = System.getProperty("tests.rest.cluster.remote.password", "x-pack-test-password");
        if (hasText(user) && hasText(pass)) {
            String token = basicAuthHeaderValue(user, new SecureString(pass.toCharArray()));
            return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
        }
        return Settings.EMPTY;
    }
}
