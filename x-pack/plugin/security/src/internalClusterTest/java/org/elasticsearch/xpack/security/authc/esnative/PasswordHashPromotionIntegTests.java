/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.esnative;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.xpack.core.security.action.user.PutUserAction;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequest;
import org.elasticsearch.xpack.core.security.action.user.PutUserResponse;
import org.elasticsearch.xpack.core.security.user.ElasticUser;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class PasswordHashPromotionIntegTests extends SecuritySingleNodeTestCase {

    @Override
    public Settings nodeSettings() {
        Settings customSettings = customSecuritySettingsSource.nodeSettings(0, Settings.EMPTY);
        MockSecureSettings mockSecuritySettings = new MockSecureSettings();
        mockSecuritySettings.setString("autoconfiguration.password_hash",
            "{PBKDF2_STRETCH}1000$JnmgicthPZkczB8MaQeJiV6IX43h7mSfPSzESqnYYSA=$OZKH5XFNK+M65mcKal6zgugWRcpl6wUXmSQZ6hPy+iw=");
        Settings.Builder builder = Settings.builder().put(customSettings, false); // don't bring in bootstrap.password
        builder.setSecureSettings(mockSecuritySettings);
        return builder.build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected boolean transportSSLEnabled() { return false; }

    public void testAuthenticate() {
        ClusterHealthResponse response = client()
            .filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(ElasticUser.NAME,
                new SecureString("password1".toCharArray()))))
            .admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes(Integer.toString(1))
            .setWaitForGreenStatus()
            .get();

        assertThat(response, notNullValue());
        assertThat(response.isTimedOut(), equalTo(false));
        assertThat(response.status(), equalTo(RestStatus.OK));
        assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        assertTrue(securityIndexExists());

        // Now as the document exists let's try to authenticate again
        response = client()
            .filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(ElasticUser.NAME,
                new SecureString("password1".toCharArray()))))
            .admin()
            .cluster()
            .prepareHealth()
            .get();

        assertThat(response, notNullValue());
        assertThat(response.status(), equalTo(RestStatus.OK));
    }

    public void testInvalidPasswordHashNoSecurityIndex() {
        ElasticsearchStatusException e = expectThrows( ElasticsearchStatusException.class,
            () -> client()
            .filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(ElasticUser.NAME,
                new SecureString("password".toCharArray()))))
            .admin()
            .cluster()
            .prepareHealth()
            .get() );

        assertThat(e.status(), equalTo(RestStatus.UNAUTHORIZED));
        assertFalse(securityIndexExists());
    }

    public void testSecurityIndexExistsButElasticuserNot() throws Exception {
        // Create a user to create the Index
        createUser("user", SecuritySettingsSource.TEST_PASSWORD_HASHED.toCharArray(), Strings.EMPTY_ARRAY);
        assertTrue(securityIndexExists());

        ClusterHealthResponse response = client()
            .filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(ElasticUser.NAME,
                new SecureString("password1".toCharArray()))))
            .admin()
            .cluster()
            .prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus()
            .get();
        assertThat(response, notNullValue());
        assertThat(response.isTimedOut(), equalTo(false));
        assertThat(response.status(), equalTo(RestStatus.OK));
    }

    // TODO: Add a test a test where we index the document for the elastic user manually and then we try to authenticate with the
    // autoconfigured password hash and it fails

    private void createUser(String username, char[] password, String[] roles) throws ExecutionException, InterruptedException {
        final PutUserRequest putUserRequest = new PutUserRequest();
        putUserRequest.username(username);
        putUserRequest.roles(roles);
        putUserRequest.passwordHash(password);
        PlainActionFuture<PutUserResponse> listener = new PlainActionFuture<>();
        final Client client = client().filterWithHeader(
            Map.of("Authorization", basicAuthHeaderValue("test_user", new SecureString("x-pack-test-password"
                .toCharArray()))));
        client.execute(PutUserAction.INSTANCE, putUserRequest, listener);
        final PutUserResponse putUserResponse = listener.get();
        assertTrue(putUserResponse.created());
    }

    private boolean securityIndexExists () {
        ClusterHealthResponse response = client()
            .filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue("test_user",
                new SecureString("x-pack-test-password".toCharArray()))))
            .admin()
            .cluster()
            .prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus()
            .get();

        boolean securityIndexExists = false;
        for (Map.Entry<String, ClusterIndexHealth>  indexEntry: response.getIndices().entrySet()) {
            if (indexEntry.getKey().startsWith(".security")) {
                securityIndexExists = true;
                break;
            }
        }
        return securityIndexExists;
    }
}
