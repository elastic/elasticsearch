/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public abstract class AbstractRemoteClusterSecurityTestCase extends ESRestTestCase {

    protected static final String USER = "test_user";
    protected static final SecureString PASS = new SecureString("x-pack-test-password".toCharArray());
    protected static final String REMOTE_SEARCH_USER = "remote_search_user";
    protected static final String REMOTE_METRIC_USER = "remote_metric_user";
    protected static final String REMOTE_TRANSFORM_USER = "remote_transform_user";
    protected static final String REMOTE_SEARCH_ROLE = "remote_search";
    protected static final String REMOTE_CLUSTER_ALIAS = "my_remote_cluster";

    protected static LocalClusterConfigProvider commonClusterConfig = cluster -> cluster.module("analysis-common")
        .feature(FeatureFlag.NEW_RCS_MODE)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.authc.token.enabled", "true")
        .setting("xpack.security.authc.api_key.enabled", "true")
        .setting("xpack.security.http.ssl.enabled", "false")
        .setting("xpack.security.transport.ssl.enabled", "true")
        .setting("xpack.security.transport.ssl.key", "transport.key")
        .setting("xpack.security.transport.ssl.certificate", "transport.crt")
        .setting("xpack.security.transport.ssl.certificate_authorities", "transport-ca.crt")
        .setting("xpack.security.transport.ssl.verification_mode", "certificate")
        .keystore("bootstrap.password", "x-pack-test-password")
        .keystore("xpack.security.transport.ssl.secure_key_passphrase", "transport-password")
        .configFile("transport.key", Resource.fromClasspath("ssl/transport.key"))
        .configFile("transport.crt", Resource.fromClasspath("ssl/transport.crt"))
        .configFile("transport-ca.crt", Resource.fromClasspath("ssl/transport-ca.crt"))
        .configFile("remote-cluster.key", Resource.fromClasspath("ssl/remote_cluster.key"))
        .configFile("remote-cluster.crt", Resource.fromClasspath("ssl/remote_cluster.crt"))
        .configFile("remote-cluster-ca.crt", Resource.fromClasspath("ssl/remote-cluster-ca.crt"))
        .configFile("remote-cluster-client.key", Resource.fromClasspath("ssl/remote-cluster-client.key"))
        .configFile("remote-cluster-client.crt", Resource.fromClasspath("ssl/remote-cluster-client.crt"))
        .configFile("remote-cluster-client-ca.crt", Resource.fromClasspath("ssl/remote-cluster-client-ca.crt"))
        .user(USER, PASS.toString());

    protected static ElasticsearchCluster fulfillingCluster;
    protected static ElasticsearchCluster queryCluster;
    protected static RestClient fulfillingClusterClient;

    @BeforeClass
    public static void initFulfillingClusterClient() {
        if (fulfillingClusterClient != null) {
            return;
        }
        fulfillingClusterClient = buildRestClient(fulfillingCluster);
    }

    static RestClient buildRestClient(ElasticsearchCluster targetCluster) {
        assert targetCluster != null;
        final int numberOfFcNodes = targetCluster.getHttpAddresses().split(",").length;
        final String url = targetCluster.getHttpAddress(randomIntBetween(0, numberOfFcNodes - 1));

        final int portSeparator = url.lastIndexOf(':');
        final var httpHost = new HttpHost(url.substring(0, portSeparator), Integer.parseInt(url.substring(portSeparator + 1)), "http");
        RestClientBuilder builder = RestClient.builder(httpHost);
        try {
            configureClient(builder, Settings.EMPTY);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        builder.setStrictDeprecationMode(true);
        return builder.build();
    }

    @AfterClass
    public static void closeFulfillingClusterClient() throws IOException {
        try {
            IOUtils.close(fulfillingClusterClient);
        } finally {
            fulfillingClusterClient = null;
        }
    }

    @Override
    protected String getTestRestCluster() {
        return queryCluster.getHttpAddress(0);
    }

    @Override
    protected Settings restClientSettings() {
        final String token = basicAuthHeaderValue(USER, PASS);
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    protected static Map<String, Object> createCrossClusterAccessApiKey(String accessJson) {
        initFulfillingClusterClient();
        return createCrossClusterAccessApiKey(fulfillingClusterClient, accessJson);
    }

    protected static Map<String, Object> createCrossClusterAccessApiKey(RestClient targetClusterClient, String accessJson) {

        // Create API key on FC
        final var createCrossClusterApiKeyRequest = new Request("POST", "/_security/cross_cluster/api_key");
        createCrossClusterApiKeyRequest.setJsonEntity(Strings.format("""
            {
              "name": "cross_cluster_access_key",
              "access": %s
            }""", accessJson));
        try {
            final Response createCrossClusterApiKeyResponse = performRequestWithAdminUser(
                targetClusterClient,
                createCrossClusterApiKeyRequest
            );
            assertOK(createCrossClusterApiKeyResponse);
            return responseAsMap(createCrossClusterApiKeyResponse);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected void configureRemoteCluster() throws Exception {
        configureRemoteCluster(fulfillingCluster, randomBoolean());
    }

    protected void configureRemoteCluster(boolean isProxyMode) throws Exception {
        configureRemoteCluster(fulfillingCluster, isProxyMode);
    }

    protected void configureRemoteCluster(ElasticsearchCluster targetFulfillingCluster, boolean isProxyMode) throws Exception {
        configureRemoteCluster(REMOTE_CLUSTER_ALIAS, targetFulfillingCluster, false, isProxyMode, false);
    }

    protected void configureRemoteCluster(String clusterAlias) throws Exception {
        configureRemoteCluster(clusterAlias, fulfillingCluster, false, randomBoolean(), false);
    }

    protected void configureRemoteCluster(
        String clusterAlias,
        ElasticsearchCluster targetFulfillingCluster,
        boolean basicSecurity,
        boolean isProxyMode,
        boolean skipUnavailable
    ) throws Exception {
        // For configurable remote cluster security, this method assumes the cross cluster access API key is already configured in keystore
        final Settings.Builder builder = Settings.builder();
        final String remoteClusterEndpoint = basicSecurity
            ? targetFulfillingCluster.getTransportEndpoint(0)
            : targetFulfillingCluster.getRemoteClusterServerEndpoint(0);
        if (isProxyMode) {
            builder.put("cluster.remote." + clusterAlias + ".mode", "proxy")
                .put("cluster.remote." + clusterAlias + ".proxy_address", remoteClusterEndpoint);
        } else {
            builder.put("cluster.remote." + clusterAlias + ".mode", "sniff")
                .putList("cluster.remote." + clusterAlias + ".seeds", remoteClusterEndpoint);
        }
        builder.put("cluster.remote." + clusterAlias + ".skip_unavailable", skipUnavailable);
        updateClusterSettings(builder.build());

        // Ensure remote cluster is connected
        final int numberOfFcNodes = targetFulfillingCluster.getHttpAddresses().split(",").length;
        final Request remoteInfoRequest = new Request("GET", "/_remote/info");
        assertBusy(() -> {
            final Response remoteInfoResponse = adminClient().performRequest(remoteInfoRequest);
            assertOK(remoteInfoResponse);
            final ObjectPath remoteInfoObjectPath = assertOKAndCreateObjectPath(remoteInfoResponse);
            assertThat(remoteInfoObjectPath.evaluate(clusterAlias + ".connected"), is(true));
            if (false == isProxyMode) {
                assertThat(remoteInfoObjectPath.evaluate(clusterAlias + ".num_nodes_connected"), equalTo(numberOfFcNodes));
            }
            final String credentialsValue = remoteInfoObjectPath.evaluate(clusterAlias + ".cluster_credentials");
            if (basicSecurity) {
                assertThat(credentialsValue, nullValue());
            } else {
                assertThat(credentialsValue, equalTo("::es_redacted::"));
            }
        });
    }

    protected static Response performRequestAgainstFulfillingCluster(Request request) throws IOException {
        return performRequestWithAdminUser(fulfillingClusterClient, request);
    }

    protected static Response performRequestWithAdminUser(RestClient targetFulfillingClusterClient, Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(USER, PASS)));
        return targetFulfillingClusterClient.performRequest(request);
    }

    // TODO centralize common usage of this across all tests
    protected static String randomEncodedApiKey() {
        return Base64.getEncoder().encodeToString((UUIDs.base64UUID() + ":" + UUIDs.base64UUID()).getBytes(StandardCharsets.UTF_8));
    }

    protected record TestClusterConfigProviders(LocalClusterConfigProvider server, LocalClusterConfigProvider client) {}

    protected static TestClusterConfigProviders EMPTY_CONFIG_PROVIDERS = new TestClusterConfigProviders(cluster -> {}, cluster -> {});
}
