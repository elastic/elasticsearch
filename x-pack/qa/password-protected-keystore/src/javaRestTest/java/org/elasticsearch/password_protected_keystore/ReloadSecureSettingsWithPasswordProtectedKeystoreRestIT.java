/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.password_protected_keystore;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ObjectPath;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class ReloadSecureSettingsWithPasswordProtectedKeystoreRestIT extends ESRestTestCase {

    private static final String KEYSTORE_PASSWORD = "keystore-password";
    private static final int NUM_NODES = 2;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .nodes(NUM_NODES)
        .keystorePassword(KEYSTORE_PASSWORD)
        .name("javaRestTest")
        .keystore(nodeSpec -> Map.of("xpack.security.transport.ssl.secure_key_passphrase", "transport-password"))
        .setting("xpack.security.enabled", "true")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.security.authc.anonymous.roles", "anonymous")
        .setting("xpack.security.transport.ssl.enabled", "true")
        .setting("xpack.security.transport.ssl.certificate", "transport.crt")
        .setting("xpack.security.transport.ssl.key", "transport.key")
        .setting("xpack.security.transport.ssl.certificate_authorities", "ca.crt")
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .configFile("transport.key", Resource.fromClasspath("ssl/transport.key"))
        .configFile("transport.crt", Resource.fromClasspath("ssl/transport.crt"))
        .configFile("ca.crt", Resource.fromClasspath("ssl/ca.crt"))
        .user("admin_user", "admin-password")
        .user("test-user", "test-user-password", "user_role", false)
        .user("manage-user", "test-user-password", "manage_role", false)
        .user("manage-security-user", "test-user-password", "manage_security_role", false)
        .user("monitor-user", "test-user-password", "monitor_role", false)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @SuppressWarnings("unchecked")
    public void testReloadSecureSettingsWithCorrectPassword() throws Exception {
        final Request request = new Request("POST", "_nodes/reload_secure_settings");
        request.setJsonEntity("{\"secure_settings_password\":\"" + KEYSTORE_PASSWORD + "\"}");
        final Response response = client().performRequest(request);
        final Map<String, Object> map = entityAsMap(response);
        assertThat(ObjectPath.eval("cluster_name", map), equalTo("javaRestTest"));
        assertThat(map.get("nodes"), instanceOf(Map.class));
        final Map<String, Object> nodes = (Map<String, Object>) map.get("nodes");
        assertThat(nodes.size(), equalTo(NUM_NODES));
        for (Map.Entry<String, Object> entry : nodes.entrySet()) {
            assertThat(entry.getValue(), instanceOf(Map.class));
            final Map<String, Object> node = (Map<String, Object>) entry.getValue();
            assertThat(node.get("reload_exception"), nullValue());
        }
    }

    @SuppressWarnings("unchecked")
    public void testReloadSecureSettingsWithDifferentPrivileges() throws Exception {
        final Request request = new Request("POST", "/_nodes/reload_secure_settings");
        request.setJsonEntity("{\"secure_settings_password\":\"" + KEYSTORE_PASSWORD + "\"}");
        final Response response = performRequestWithUser("manage-user", request);
        final Map<String, Object> map = entityAsMap(response);
        assertThat(ObjectPath.eval("cluster_name", map), equalTo("javaRestTest"));
        assertThat(map.get("nodes"), instanceOf(Map.class));
        final Map<String, Object> nodes = (Map<String, Object>) map.get("nodes");
        assertThat(nodes.size(), equalTo(NUM_NODES));
        for (Map.Entry<String, Object> entry : nodes.entrySet()) {
            assertThat(entry.getValue(), instanceOf(Map.class));
            final Map<String, Object> node = (Map<String, Object>) entry.getValue();
            assertThat(node.get("reload_exception"), nullValue());
        }
        expectThrows403(() -> {
            final Request innerRequest = new Request("POST", "/_nodes/reload_secure_settings");
            innerRequest.setJsonEntity("{\"secure_settings_password\":\"" + KEYSTORE_PASSWORD + "\"}");
            performRequestWithUser("manage-security-user", innerRequest);
        });
        expectThrows403(() -> {
            final Request innerRequest = new Request("POST", "/_nodes/reload_secure_settings");
            innerRequest.setJsonEntity("{\"secure_settings_password\":\"" + KEYSTORE_PASSWORD + "\"}");
            performRequestWithUser("monitor-user", request);
        });
    }

    @SuppressWarnings("unchecked")
    public void testReloadSecureSettingsWithIncorrectPassword() throws Exception {
        final Request request = new Request("POST", "_nodes/reload_secure_settings");
        request.setJsonEntity("{\"secure_settings_password\":\"" + KEYSTORE_PASSWORD + randomAlphaOfLength(7) + "\"}");
        final Response response = client().performRequest(request);
        final Map<String, Object> map = entityAsMap(response);
        assertThat(ObjectPath.eval("cluster_name", map), equalTo("javaRestTest"));
        assertThat(map.get("nodes"), instanceOf(Map.class));
        final Map<String, Object> nodes = (Map<String, Object>) map.get("nodes");
        assertThat(nodes.size(), equalTo(NUM_NODES));
        for (Map.Entry<String, Object> entry : nodes.entrySet()) {
            assertThat(entry.getValue(), instanceOf(Map.class));
            final Map<String, Object> node = (Map<String, Object>) entry.getValue();
            assertThat(node.get("reload_exception"), instanceOf(Map.class));
            assertThat(
                ObjectPath.eval("reload_exception.reason", node),
                anyOf(equalTo("Provided keystore password was incorrect"), equalTo("Keystore has been corrupted or tampered with"))
            );
            assertThat(ObjectPath.eval("reload_exception.type", node), equalTo("security_exception"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testReloadSecureSettingsWithEmptyPassword() throws Exception {
        final Request request = new Request("POST", "_nodes/reload_secure_settings");
        final Response response = client().performRequest(request);
        final Map<String, Object> map = entityAsMap(response);
        assertThat(ObjectPath.eval("cluster_name", map), equalTo("javaRestTest"));
        assertThat(map.get("nodes"), instanceOf(Map.class));
        final Map<String, Object> nodes = (Map<String, Object>) map.get("nodes");
        assertThat(nodes.size(), equalTo(NUM_NODES));
        for (Map.Entry<String, Object> entry : nodes.entrySet()) {
            assertThat(entry.getValue(), instanceOf(Map.class));
            final Map<String, Object> node = (Map<String, Object>) entry.getValue();
            assertThat(node.get("reload_exception"), instanceOf(Map.class));
            assertThat(
                ObjectPath.eval("reload_exception.reason", node),
                anyOf(
                    equalTo("Provided keystore password was incorrect"),
                    equalTo("Keystore has been corrupted or tampered with"),
                    containsString("Error generating an encryption key from the provided password") // FIPS
                )
            );
            assertThat(
                ObjectPath.eval("reload_exception.type", node),
                // Depends on exact security provider (eg Sun vs BCFIPS)
                anyOf(equalTo("security_exception"), equalTo("general_security_exception"))
            );
        }
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test-user", new SecureString("test-user-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private static void expectThrows403(ThrowingRunnable runnable) {
        assertThat(expectThrows(ResponseException.class, runnable).getResponse().getStatusLine().getStatusCode(), equalTo(403));
    }

    private Response performRequestWithUser(final String username, final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue(username, new SecureString("test-user-password")))
        );
        return client().performRequest(request);
    }
}
