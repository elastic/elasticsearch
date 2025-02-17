/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.esnative;

import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.junit.BeforeClass;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;

public class ReservedRealmElasticAutoconfigIntegTests extends SecuritySingleNodeTestCase {

    private static Hasher hasher;

    @BeforeClass
    public static void setHasher() {
        hasher = getFastStoredHashAlgoForTests();
    }

    @Override
    public Settings nodeSettings() {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(super.nodeSettings())
            .put("xpack.security.authc.password_hashing.algorithm", hasher.name());
        ((MockSecureSettings) settingsBuilder.getSecureSettings()).setString(
            "autoconfiguration.password_hash",
            new String(hasher.hash(new SecureString("auto_password_that_is_longer_than_14_chars_because_of_FIPS".toCharArray())))
        );
        return settingsBuilder.build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable HTTP
    }

    @Override
    protected SecureString getBootstrapPassword() {
        return null; // no bootstrap password for this test
    }

    public void testAutoconfigFailedPasswordPromotion() throws Exception {
        try {
            // .security index is created automatically on node startup so delete the security index first
            deleteSecurityIndexIfExists();
            // prevents the .security index from being created automatically (after elastic user authentication)
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT
            );
            updateSettingsRequest.transientSettings(Settings.builder().put(Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true));
            assertAcked(clusterAdmin().updateSettings(updateSettingsRequest).actionGet());

            // elastic user gets 503 for the good password
            Request restRequest = randomFrom(
                new Request("GET", "/_security/_authenticate"),
                new Request("GET", "_cluster/health"),
                new Request("GET", "_nodes")
            );
            RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
            options.addHeader(
                UsernamePasswordToken.BASIC_AUTH_HEADER,
                UsernamePasswordToken.basicAuthHeaderValue(
                    "elastic",
                    new SecureString("auto_password_that_is_longer_than_14_chars_because_of_FIPS".toCharArray())
                )
            );
            restRequest.setOptions(options);
            ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(restRequest));
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(RestStatus.SERVICE_UNAVAILABLE.getStatus()));

            // but gets a 401 for the wrong password
            Request restRequest2 = randomFrom(
                new Request("GET", "/_security/_authenticate"),
                new Request("GET", "_cluster/health"),
                new Request("GET", "_nodes")
            );
            options = RequestOptions.DEFAULT.toBuilder();
            options.addHeader(
                UsernamePasswordToken.BASIC_AUTH_HEADER,
                UsernamePasswordToken.basicAuthHeaderValue(
                    "elastic",
                    new SecureString("wrong password_that_is_longer_than_14_chars_because_of_FIPS".toCharArray())
                )
            );
            restRequest2.setOptions(options);
            exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(restRequest2));
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(RestStatus.UNAUTHORIZED.getStatus()));
        } finally {
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT
            );
            updateSettingsRequest.transientSettings(
                Settings.builder().put(Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), (String) null)
            );
            assertAcked(clusterAdmin().updateSettings(updateSettingsRequest).actionGet());
        }
    }

    public void testAutoconfigSucceedsAfterPromotionFailure() throws Exception {
        try {
            // create any non-elastic user, which triggers .security index creation
            final PutUserRequest putUserRequest = new PutUserRequest();
            final String username = randomAlphaOfLength(8);
            putUserRequest.username(username);
            final SecureString password = new SecureString("super-strong-password!".toCharArray());
            putUserRequest.passwordHash(Hasher.PBKDF2.hash(password));
            putUserRequest.roles(Strings.EMPTY_ARRAY);
            client().execute(PutUserAction.INSTANCE, putUserRequest).get();
            // Security migration needs to finish before making the cluster read only
            awaitSecurityMigration();

            // but then make the cluster read-only
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT
            );
            updateSettingsRequest.transientSettings(Settings.builder().put(Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true));
            assertAcked(clusterAdmin().updateSettings(updateSettingsRequest).actionGet());

            // elastic user now gets 503 for the good password
            Request restRequest = randomFrom(
                new Request("GET", "/_security/_authenticate"),
                new Request("GET", "_cluster/health"),
                new Request("GET", "_nodes")
            );
            RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
            options.addHeader(
                UsernamePasswordToken.BASIC_AUTH_HEADER,
                UsernamePasswordToken.basicAuthHeaderValue(
                    "elastic",
                    new SecureString("auto_password_that_is_longer_than_14_chars_because_of_FIPS".toCharArray())
                )
            );
            restRequest.setOptions(options);
            ResponseException exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(restRequest));
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(RestStatus.SERVICE_UNAVAILABLE.getStatus()));
            // clear cluster-wide write block
            updateSettingsRequest = new ClusterUpdateSettingsRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
            updateSettingsRequest.transientSettings(
                Settings.builder().put(Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), (String) null)
            );
            assertAcked(clusterAdmin().updateSettings(updateSettingsRequest).actionGet());

            if (randomBoolean()) {
                Request restRequest2 = randomFrom(
                    new Request("GET", "/_security/_authenticate"),
                    new Request("GET", "_cluster/health"),
                    new Request("GET", "_nodes")
                );
                options = RequestOptions.DEFAULT.toBuilder();
                options.addHeader(
                    UsernamePasswordToken.BASIC_AUTH_HEADER,
                    UsernamePasswordToken.basicAuthHeaderValue(
                        "elastic",
                        new SecureString("wrong password_that_is_longer_than_14_chars_because_of_FIPS".toCharArray())
                    )
                );
                restRequest2.setOptions(options);
                exception = expectThrows(ResponseException.class, () -> getRestClient().performRequest(restRequest2));
                assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(RestStatus.UNAUTHORIZED.getStatus()));
            }

            // now the auto config password can be promoted, and authn succeeds
            Request restRequest3 = randomFrom(
                new Request("GET", "/_security/_authenticate"),
                new Request("GET", "_cluster/health"),
                new Request("GET", "_nodes")
            );
            options = RequestOptions.DEFAULT.toBuilder();
            options.addHeader(
                UsernamePasswordToken.BASIC_AUTH_HEADER,
                UsernamePasswordToken.basicAuthHeaderValue(
                    "elastic",
                    new SecureString("auto_password_that_is_longer_than_14_chars_because_of_FIPS".toCharArray())
                )
            );
            restRequest3.setOptions(options);
            assertThat(getRestClient().performRequest(restRequest3).getStatusLine().getStatusCode(), is(RestStatus.OK.getStatus()));
        } finally {
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT
            );
            updateSettingsRequest.transientSettings(
                Settings.builder().put(Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), (String) null)
            );
            assertAcked(clusterAdmin().updateSettings(updateSettingsRequest).actionGet());
        }
    }
}
