/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.password_protected_keystore;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ObjectPath;

import java.util.Map;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class ReloadSecureSettingsWithPasswordProtectedKeystoreRestIT extends ESRestTestCase {
    // From build.gradle
    private final String KEYSTORE_PASSWORD = "keystore-password";
    private final int NUM_NODES = 2;

    @SuppressWarnings("unchecked")
    public void testReloadSecureSettingsWithCorrectPassword() throws Exception {
        final Request request = new Request("POST", "_nodes/reload_secure_settings");
        request.setJsonEntity("{\"secure_settings_password\":\"" + KEYSTORE_PASSWORD + "\"}");
        final Response response = client().performRequest(request);
        final Map<String, Object> map = entityAsMap(response);
        assertThat(ObjectPath.eval("cluster_name", map), equalTo("integTest"));
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
    public void testReloadSecureSettingsWithIncorrectPassword() throws Exception {
        final Request request = new Request("POST", "_nodes/reload_secure_settings");
        request.setJsonEntity("{\"secure_settings_password\":\"" + KEYSTORE_PASSWORD + randomAlphaOfLength(7) + "\"}");
        final Response response = client().performRequest(request);
        final Map<String, Object> map = entityAsMap(response);
        assertThat(ObjectPath.eval("cluster_name", map), equalTo("integTest"));
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
        assertThat(ObjectPath.eval("cluster_name", map), equalTo("integTest"));
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
}
