/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.Request;
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
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public abstract class AbstractRemoteClusterSecurityTestCase extends ESRestTestCase {

    protected static final String USER = "test_user";
    protected static final SecureString PASS = new SecureString("x-pack-test-password".toCharArray());
    protected static final String REMOTE_SEARCH_USER = "remote_search_user";
    protected static final String MANAGE_USER = "manage_user";
    protected static final String REMOTE_METRIC_USER = "remote_metric_user";
    protected static final String REMOTE_TRANSFORM_USER = "remote_transform_user";
    protected static final String REMOTE_SEARCH_ROLE = "remote_search";
    protected static final String REMOTE_CLUSTER_ALIAS = "my_remote_cluster";
    static final String KEYSTORE_PASSWORD = "keystore-password";

    protected static LocalClusterConfigProvider commonClusterConfig = cluster -> cluster.module("analysis-common")
        .keystorePassword(KEYSTORE_PASSWORD)
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
        .module("reindex") // Needed for the role metadata migration
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
            doConfigureClient(builder, Settings.EMPTY);
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

    protected static String headerFromRandomAuthMethod(final String username, final SecureString password) throws IOException {
        final boolean useBearerTokenAuth = randomBoolean();
        if (useBearerTokenAuth) {
            final Request request = new Request(HttpPost.METHOD_NAME, "/_security/oauth2/token");
            request.setJsonEntity(String.format(Locale.ROOT, """
                {
                  "grant_type":"password",
                  "username":"%s",
                  "password":"%s"
                }
                """, username, password));
            final Map<String, Object> responseBody = entityAsMap(adminClient().performRequest(request));
            return "Bearer " + responseBody.get("access_token");
        } else {
            return basicAuthHeaderValue(username, password);
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
        putRemoteClusterSettings(clusterAlias, targetFulfillingCluster, basicSecurity, isProxyMode, skipUnavailable);

        // Ensure remote cluster is connected
        checkRemoteConnection(clusterAlias, targetFulfillingCluster, basicSecurity, isProxyMode);
    }

    protected void configureRemoteClusterCredentials(String clusterAlias, String credentials, MutableSettingsProvider keystoreSettings)
        throws IOException {
        keystoreSettings.put("cluster.remote." + clusterAlias + ".credentials", credentials);
        queryCluster.updateStoredSecureSettings();
        reloadSecureSettings();
    }

    protected void removeRemoteClusterCredentials(String clusterAlias, MutableSettingsProvider keystoreSettings) throws IOException {
        keystoreSettings.remove("cluster.remote." + clusterAlias + ".credentials");
        queryCluster.updateStoredSecureSettings();
        reloadSecureSettings();
    }

    @SuppressWarnings("unchecked")
    protected void reloadSecureSettings() throws IOException {
        final Request request = new Request("POST", "/_nodes/reload_secure_settings");
        request.setJsonEntity("{\"secure_settings_password\":\"" + KEYSTORE_PASSWORD + "\"}");
        final Response reloadResponse = adminClient().performRequest(request);
        assertOK(reloadResponse);
        final Map<String, Object> map = entityAsMap(reloadResponse);
        assertThat(map.get("nodes"), instanceOf(Map.class));
        final Map<String, Object> nodes = (Map<String, Object>) map.get("nodes");
        assertThat(nodes, is(not(anEmptyMap())));
        for (Map.Entry<String, Object> entry : nodes.entrySet()) {
            assertThat(entry.getValue(), instanceOf(Map.class));
            final Map<String, Object> node = (Map<String, Object>) entry.getValue();
            assertThat(node.get("reload_exception"), nullValue());
        }
    }

    protected void putRemoteClusterSettings(
        String clusterAlias,
        ElasticsearchCluster targetFulfillingCluster,
        boolean basicSecurity,
        boolean isProxyMode,
        boolean skipUnavailable
    ) throws IOException {
        final Settings.Builder builder = Settings.builder();
        final String remoteClusterEndpoint = basicSecurity
            ? targetFulfillingCluster.getTransportEndpoint(0)
            : targetFulfillingCluster.getRemoteClusterServerEndpoint(0);
        if (isProxyMode) {
            builder.put("cluster.remote." + clusterAlias + ".mode", "proxy")
                .put("cluster.remote." + clusterAlias + ".proxy_address", remoteClusterEndpoint)
                .putNull("cluster.remote." + clusterAlias + ".seeds");
        } else {
            builder.put("cluster.remote." + clusterAlias + ".mode", "sniff")
                .putList("cluster.remote." + clusterAlias + ".seeds", remoteClusterEndpoint)
                .putNull("cluster.remote." + clusterAlias + ".proxy_address");
        }
        builder.put("cluster.remote." + clusterAlias + ".skip_unavailable", skipUnavailable);
        updateClusterSettings(builder.build());
    }

    protected void checkRemoteConnection(
        String clusterAlias,
        ElasticsearchCluster targetFulfillingCluster,
        boolean basicSecurity,
        boolean isProxyMode
    ) throws Exception {
        final Request remoteInfoRequest = new Request("GET", "/_remote/info");
        assertBusy(() -> {
            final Response remoteInfoResponse = adminClient().performRequest(remoteInfoRequest);
            assertOK(remoteInfoResponse);
            final ObjectPath remoteInfoObjectPath = assertOKAndCreateObjectPath(remoteInfoResponse);
            assertThat(remoteInfoObjectPath.evaluate(clusterAlias + ".connected"), is(true));
            if (false == isProxyMode) {
                int numberOfFcNodes = (int) Arrays.stream(targetFulfillingCluster.getRemoteClusterServerEndpoints().split(","))
                    .filter(endpoint -> endpoint.length() > 0)
                    .count();
                if (numberOfFcNodes == 0) {
                    // The cluster is an RCS 1.0 remote cluster
                    numberOfFcNodes = targetFulfillingCluster.getTransportEndpoints().split(",").length;
                }
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
        request.setOptions(request.getOptions().toBuilder().addHeader("Authorization", basicAuthHeaderValue(USER, PASS)));
        return targetFulfillingClusterClient.performRequest(request);
    }

    protected static String randomEncodedApiKey() {
        return Base64.getEncoder()
            .encodeToString((UUIDs.base64UUID() + ":" + UUIDs.randomBase64UUIDSecureString()).getBytes(StandardCharsets.UTF_8));
    }

    protected record TestClusterConfigProviders(LocalClusterConfigProvider server, LocalClusterConfigProvider client) {}

    protected static TestClusterConfigProviders EMPTY_CONFIG_PROVIDERS = new TestClusterConfigProviders(cluster -> {}, cluster -> {});
}
