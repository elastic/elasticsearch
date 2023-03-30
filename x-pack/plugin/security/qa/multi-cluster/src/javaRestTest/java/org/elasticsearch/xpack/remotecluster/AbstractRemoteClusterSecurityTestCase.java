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
import org.elasticsearch.xcontent.ObjectPath;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;

public abstract class AbstractRemoteClusterSecurityTestCase extends ESRestTestCase {

    protected static final String USER = "test_user";
    protected static final SecureString PASS = new SecureString("x-pack-test-password".toCharArray());
    protected static final String REMOTE_SEARCH_USER = "remote_search_user";
    protected static final String REMOTE_METRIC_USER = "remote_metric_user";
    protected static final String REMOTE_SEARCH_ROLE = "remote_search";

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
        .user(USER, PASS.toString());

    protected static ElasticsearchCluster fulfillingCluster;
    protected static ElasticsearchCluster queryCluster;
    protected static RestClient fulfillingClusterClient;

    @BeforeClass
    public static void initFulfillingClusterClient() {
        if (fulfillingClusterClient != null) {
            return;
        }
        assert fulfillingCluster != null;
        final int numberOfFcNodes = fulfillingCluster.getHttpAddresses().split(",").length;
        final String url = fulfillingCluster.getHttpAddress(randomIntBetween(0, numberOfFcNodes - 1));

        final int portSeparator = url.lastIndexOf(':');
        final var httpHost = new HttpHost(url.substring(0, portSeparator), Integer.parseInt(url.substring(portSeparator + 1)), "http");
        RestClientBuilder builder = RestClient.builder(httpHost);
        try {
            configureClient(builder, Settings.EMPTY);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        builder.setStrictDeprecationMode(true);
        fulfillingClusterClient = builder.build();
    }

    @AfterClass
    public static void closeFulFillingClusterClient() throws IOException {
        IOUtils.close(fulfillingClusterClient);
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

    protected static Map<String, Object> createCrossClusterAccessApiKey(String indicesPrivilegesJson) {
        initFulfillingClusterClient();
        // Create API key on FC
        final var createApiKeyRequest = new Request("POST", "/_security/api_key");
        createApiKeyRequest.setJsonEntity(Strings.format("""
            {
              "name": "cross_cluster_access_key",
              "role_descriptors": {
                "role": {
                  "cluster": ["cross_cluster_access"],
                  "index": %s
                }
              }
            }""", indicesPrivilegesJson));
        try {
            final Response createApiKeyResponse = performRequestAgainstFulfillingCluster(createApiKeyRequest);
            assertOK(createApiKeyResponse);
            return responseAsMap(createApiKeyResponse);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected void configureRemoteClusters() throws Exception {
        // This method assume the cross cluster access API key is already configured in keystore
        configureRemoteClusters(randomBoolean());
    }

    /**
     * Returns API key ID of cross cluster access API key.
     */
    protected void configureRemoteClusters(boolean isProxyMode) throws Exception {
        // This method assume the cross cluster access API key is already configured in keystore
        final Settings.Builder builder = Settings.builder();
        if (isProxyMode) {
            builder.put("cluster.remote.my_remote_cluster.mode", "proxy")
                .put("cluster.remote.my_remote_cluster.proxy_address", fulfillingCluster.getRemoteClusterServerEndpoint(0));
        } else {
            builder.put("cluster.remote.my_remote_cluster.mode", "sniff")
                .putList("cluster.remote.my_remote_cluster.seeds", fulfillingCluster.getRemoteClusterServerEndpoint(0));
        }
        updateClusterSettings(builder.build());

        // Ensure remote cluster is connected
        final int numberOfFcNodes = fulfillingCluster.getHttpAddresses().split(",").length;
        final Request remoteInfoRequest = new Request("GET", "/_remote/info");
        assertBusy(() -> {
            final Response remoteInfoResponse = adminClient().performRequest(remoteInfoRequest);
            assertOK(remoteInfoResponse);
            final Map<String, Object> remoteInfoMap = responseAsMap(remoteInfoResponse);
            assertThat(remoteInfoMap, hasKey("my_remote_cluster"));
            assertThat(ObjectPath.eval("my_remote_cluster.connected", remoteInfoMap), is(true));
            if (false == isProxyMode) {
                assertThat(ObjectPath.eval("my_remote_cluster.num_nodes_connected", remoteInfoMap), equalTo(numberOfFcNodes));
            }
            assertThat(ObjectPath.eval("my_remote_cluster.cluster_credentials", remoteInfoMap), equalTo("::es_redacted::"));
        });
    }

    protected static Response performRequestAgainstFulfillingCluster(Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(USER, PASS)));
        return fulfillingClusterClient.performRequest(request);
    }

    // TODO centralize common usage of this across all tests
    protected static String randomEncodedApiKey() {
        return Base64.getEncoder().encodeToString((UUIDs.base64UUID() + ":" + UUIDs.base64UUID()).getBytes(StandardCharsets.UTF_8));
    }
}
