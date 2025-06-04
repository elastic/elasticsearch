/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ElasticsearchSecurityException;
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

    public void testAuthenticate() {
        final List<String> usernames = Arrays.asList(
            ElasticUser.NAME,
            KibanaUser.NAME,
            KibanaSystemUser.NAME,
            LogstashSystemUser.NAME,
            BeatsSystemUser.NAME,
            APMSystemUser.NAME,
            RemoteMonitoringUser.NAME
        );
        for (String username : usernames) {
            ClusterHealthResponse response = client().filterWithHeader(
                singletonMap("Authorization", basicAuthHeaderValue(username, getReservedPassword()))
            ).admin().cluster().prepareHealth(TEST_REQUEST_TIMEOUT).get();

            assertThat(response.getClusterName(), is(cluster().getClusterName()));
        }
    }

    /**
     * Enabling a user forces a doc to be written to the security index, and "user doc with empty password" has a special case code in
     * the reserved realm.
     */
    public void testAuthenticateAfterEnablingUser() throws IOException {
        final List<String> usernames = Arrays.asList(
            ElasticUser.NAME,
            KibanaUser.NAME,
            KibanaSystemUser.NAME,
            LogstashSystemUser.NAME,
            BeatsSystemUser.NAME,
            APMSystemUser.NAME,
            RemoteMonitoringUser.NAME
        );
        for (String username : usernames) {
            getSecurityClient().setUserEnabled(username, true);

            ClusterHealthResponse response = client().filterWithHeader(
                singletonMap("Authorization", basicAuthHeaderValue(username, getReservedPassword()))
            ).admin().cluster().prepareHealth(TEST_REQUEST_TIMEOUT).get();

            assertThat(response.getClusterName(), is(cluster().getClusterName()));
        }
    }

    public void testChangingPassword() throws IOException {
        String username = randomFrom(
            ElasticUser.NAME,
            KibanaUser.NAME,
            KibanaSystemUser.NAME,
            LogstashSystemUser.NAME,
            BeatsSystemUser.NAME,
            APMSystemUser.NAME,
            RemoteMonitoringUser.NAME
        );
        final char[] newPassword = "supersecretvalue".toCharArray();

        if (randomBoolean()) {
            ClusterHealthResponse response = client().filterWithHeader(
                singletonMap("Authorization", basicAuthHeaderValue(username, getReservedPassword()))
            ).admin().cluster().prepareHealth(TEST_REQUEST_TIMEOUT).get();
            assertThat(response.getClusterName(), is(cluster().getClusterName()));
        }

        getSecurityClient().changePassword(username, new SecureString(Arrays.copyOf(newPassword, newPassword.length)));

        ElasticsearchSecurityException elasticsearchSecurityException = expectThrows(
            ElasticsearchSecurityException.class,
            () -> client().filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(username, getReservedPassword())))
                .admin()
                .cluster()
                .prepareHealth(TEST_REQUEST_TIMEOUT)
                .get()
        );
        assertThat(elasticsearchSecurityException.getMessage(), containsString("authenticate"));

        ClusterHealthResponse healthResponse = client().filterWithHeader(
            singletonMap("Authorization", basicAuthHeaderValue(username, new SecureString(newPassword)))
        ).admin().cluster().prepareHealth(TEST_REQUEST_TIMEOUT).get();
        assertThat(healthResponse.getClusterName(), is(cluster().getClusterName()));
    }

    public void testDisablingUser() throws Exception {
        // validate the user works
        ClusterHealthResponse response = client().filterWithHeader(
            singletonMap("Authorization", basicAuthHeaderValue(ElasticUser.NAME, getReservedPassword()))
        ).admin().cluster().prepareHealth(TEST_REQUEST_TIMEOUT).get();
        assertThat(response.getClusterName(), is(cluster().getClusterName()));

        // disable user
        getSecurityClient().setUserEnabled(ElasticUser.NAME, false);
        ElasticsearchSecurityException elasticsearchSecurityException = expectThrows(
            ElasticsearchSecurityException.class,
            () -> client().filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(ElasticUser.NAME, getReservedPassword())))
                .admin()
                .cluster()
                .prepareHealth(TEST_REQUEST_TIMEOUT)
                .get()
        );
        assertThat(elasticsearchSecurityException.getMessage(), containsString("authenticate"));

        // enable
        getSecurityClient().setUserEnabled(ElasticUser.NAME, true);
        response = client().filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(ElasticUser.NAME, getReservedPassword())))
            .admin()
            .cluster()
            .prepareHealth(TEST_REQUEST_TIMEOUT)
            .get();
        assertThat(response.getClusterName(), is(cluster().getClusterName()));
    }
}
