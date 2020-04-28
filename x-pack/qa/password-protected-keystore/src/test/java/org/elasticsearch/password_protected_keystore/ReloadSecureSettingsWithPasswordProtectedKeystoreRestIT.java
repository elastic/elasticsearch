/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.password_protected_keystore;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.test.rest.ESRestTestCase;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

import java.util.Map;

public class ReloadSecureSettingsWithPasswordProtectedKeystoreRestIT extends ESRestTestCase {
    // From build.gradle
    private final String KEYSTORE_PASSWORD = "s3cr3t";
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
    public void testReloadSecureSettingsWithInCorrectPassword() throws Exception {
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
            assertThat(ObjectPath.eval("reload_exception.reason", node), equalTo("Provided keystore password was incorrect"));
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
            assertThat(ObjectPath.eval("reload_exception.reason", node), equalTo("Provided keystore password was incorrect"));
            assertThat(ObjectPath.eval("reload_exception.type", node), equalTo("security_exception"));
        }
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test-user", new SecureString("test-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }
}
