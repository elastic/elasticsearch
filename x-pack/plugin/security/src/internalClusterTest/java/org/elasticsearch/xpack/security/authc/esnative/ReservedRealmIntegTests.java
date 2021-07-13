/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.security.ChangePasswordRequest;
import org.elasticsearch.client.security.DisableUserRequest;
import org.elasticsearch.client.security.EnableUserRequest;
import org.elasticsearch.client.security.RefreshPolicy;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.NativeRealmIntegTestCase;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.user.APMSystemUser;
import org.elasticsearch.xpack.core.security.user.BeatsSystemUser;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.KibanaSystemUser;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.elasticsearch.xpack.core.security.user.LogstashSystemUser;
import org.elasticsearch.xpack.core.security.user.RemoteMonitoringUser;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.test.SecuritySettingsSource.SECURITY_REQUEST_OPTIONS;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * Integration tests for the built in realm
 */
public class ReservedRealmIntegTests extends NativeRealmIntegTestCase {

    private static Hasher hasher;

    @BeforeClass
    public static void setHasher() {
        hasher = getFastStoredHashAlgoForTests();
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings settings = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("xpack.security.authc.password_hashing.algorithm", hasher.name())
            .build();
        return settings;
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public void testAuthenticate() {
        final List<String> usernames = Arrays.asList(ElasticUser.NAME, KibanaUser.NAME, KibanaSystemUser.NAME,
            LogstashSystemUser.NAME, BeatsSystemUser.NAME, APMSystemUser.NAME, RemoteMonitoringUser.NAME);
        for (String username : usernames) {
            ClusterHealthResponse response = client()
                    .filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(username, getReservedPassword())))
                    .admin()
                    .cluster()
                    .prepareHealth()
                    .get();

            assertThat(response.getClusterName(), is(cluster().getClusterName()));
        }
    }

    /**
     * Enabling a user forces a doc to be written to the security index, and "user doc with empty password" has a special case code in
     * the reserved realm.
     */
    public void testAuthenticateAfterEnablingUser() throws IOException {
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        final List<String> usernames = Arrays.asList(ElasticUser.NAME, KibanaUser.NAME, KibanaSystemUser.NAME,
            LogstashSystemUser.NAME, BeatsSystemUser.NAME, APMSystemUser.NAME, RemoteMonitoringUser.NAME);
        for (String username : usernames) {
            restClient.security().enableUser(new EnableUserRequest(username, RefreshPolicy.getDefault()), SECURITY_REQUEST_OPTIONS);
            ClusterHealthResponse response = client()
                    .filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(username, getReservedPassword())))
                    .admin()
                    .cluster()
                    .prepareHealth()
                    .get();

            assertThat(response.getClusterName(), is(cluster().getClusterName()));
        }
    }

    public void testChangingPassword() throws IOException {
        String username = randomFrom(ElasticUser.NAME, KibanaUser.NAME, KibanaSystemUser.NAME,
            LogstashSystemUser.NAME, BeatsSystemUser.NAME, APMSystemUser.NAME, RemoteMonitoringUser.NAME);
        final char[] newPassword = "supersecretvalue".toCharArray();

        if (randomBoolean()) {
            ClusterHealthResponse response = client()
                    .filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(username, getReservedPassword())))
                    .admin()
                    .cluster()
                    .prepareHealth()
                    .get();
            assertThat(response.getClusterName(), is(cluster().getClusterName()));
        }

        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        final boolean changed = restClient.security()
            .changePassword(new ChangePasswordRequest(username, Arrays.copyOf(newPassword, newPassword.length), RefreshPolicy.IMMEDIATE),
                SECURITY_REQUEST_OPTIONS);
        assertTrue(changed);

        ElasticsearchSecurityException elasticsearchSecurityException = expectThrows(ElasticsearchSecurityException.class, () -> client()
                    .filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(username, getReservedPassword())))
                    .admin()
                    .cluster()
                    .prepareHealth()
                    .get());
        assertThat(elasticsearchSecurityException.getMessage(), containsString("authenticate"));

        ClusterHealthResponse healthResponse = client()
                .filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(username, new SecureString(newPassword))))
                .admin()
                .cluster()
                .prepareHealth()
                .get();
        assertThat(healthResponse.getClusterName(), is(cluster().getClusterName()));
    }

    public void testDisablingUser() throws Exception {
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        // validate the user works
        ClusterHealthResponse response = client()
                .filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(ElasticUser.NAME, getReservedPassword())))
                .admin()
                .cluster()
                .prepareHealth()
                .get();
        assertThat(response.getClusterName(), is(cluster().getClusterName()));

        // disable user
        final boolean disabled =
            restClient.security().disableUser(new DisableUserRequest(ElasticUser.NAME, RefreshPolicy.getDefault()),
                SECURITY_REQUEST_OPTIONS);
        assertTrue(disabled);
        ElasticsearchSecurityException elasticsearchSecurityException = expectThrows(ElasticsearchSecurityException.class, () -> client()
                .filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(ElasticUser.NAME, getReservedPassword())))
                .admin()
                .cluster()
                .prepareHealth()
                .get());
        assertThat(elasticsearchSecurityException.getMessage(), containsString("authenticate"));

        //enable
        final boolean enabled =
            restClient.security().enableUser(new EnableUserRequest(ElasticUser.NAME, RefreshPolicy.getDefault()), SECURITY_REQUEST_OPTIONS);
        assertTrue(enabled);
        response = client()
                .filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(ElasticUser.NAME, getReservedPassword())))
                .admin()
                .cluster()
                .prepareHealth()
                .get();
        assertThat(response.getClusterName(), is(cluster().getClusterName()));
    }
}
