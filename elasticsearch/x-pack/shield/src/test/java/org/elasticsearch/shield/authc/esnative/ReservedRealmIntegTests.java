/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.esnative;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.shield.user.KibanaUser;
import org.elasticsearch.shield.user.XPackUser;
import org.elasticsearch.shield.action.user.ChangePasswordResponse;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.test.NativeRealmIntegTestCase;

import java.util.Arrays;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for the built in realm
 */
public class ReservedRealmIntegTests extends NativeRealmIntegTestCase {

    private static final SecuredString DEFAULT_PASSWORD = new SecuredString("changeme".toCharArray());

    public void testAuthenticate() {
        for (String username : Arrays.asList(XPackUser.NAME, KibanaUser.NAME)) {
            ClusterHealthResponse response = client()
                    .filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(username, DEFAULT_PASSWORD)))
                    .admin()
                    .cluster()
                    .prepareHealth()
                    .get();

            assertThat(response.getClusterName(), is(cluster().getClusterName()));
        }
    }

    public void testChangingPassword() {
        String username = randomFrom(XPackUser.NAME, KibanaUser.NAME);
        final char[] newPassword = "supersecretvalue".toCharArray();

        if (randomBoolean()) {
            ClusterHealthResponse response = client()
                    .filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(username, DEFAULT_PASSWORD)))
                    .admin()
                    .cluster()
                    .prepareHealth()
                    .get();
            assertThat(response.getClusterName(), is(cluster().getClusterName()));
        }

        ChangePasswordResponse response = securityClient().prepareChangePassword(username, newPassword).get();
        assertThat(response, notNullValue());

        ElasticsearchSecurityException elasticsearchSecurityException = expectThrows(ElasticsearchSecurityException.class, () -> client()
                    .filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(username, DEFAULT_PASSWORD)))
                    .admin()
                    .cluster()
                    .prepareHealth()
                    .get());
        assertThat(elasticsearchSecurityException.getMessage(), containsString("authenticate"));

        ClusterHealthResponse healthResponse = client()
                .filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(username, new SecuredString(newPassword))))
                .admin()
                .cluster()
                .prepareHealth()
                .get();
        assertThat(healthResponse.getClusterName(), is(cluster().getClusterName()));
    }
}
