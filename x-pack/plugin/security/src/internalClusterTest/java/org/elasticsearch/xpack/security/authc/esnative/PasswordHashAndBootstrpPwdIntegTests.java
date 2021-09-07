/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.esnative;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.xpack.core.security.user.ElasticUser;

import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class PasswordHashAndBootstrpPwdIntegTests extends SecuritySingleNodeTestCase {

    @Override
    public Settings nodeSettings() {
        Settings customSettings = customSecuritySettingsSource.nodeSettings(0, Settings.EMPTY);
        MockSecureSettings mockSecuritySettings = new MockSecureSettings();
        mockSecuritySettings.setString("autoconfiguration.password_hash", // password1
            "{PBKDF2_STRETCH}1000$JnmgicthPZkczB8MaQeJiV6IX43h7mSfPSzESqnYYSA=$OZKH5XFNK+M65mcKal6zgugWRcpl6wUXmSQZ6hPy+iw=");
        mockSecuritySettings.setString("bootstrap.password", "password");
        Settings.Builder builder = Settings.builder().put(customSettings, true);
        builder.setSecureSettings(mockSecuritySettings);
        return builder.build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected boolean transportSSLEnabled() { return false; }

    public void testBootstrapPwdAuthenticatePwdHashNotIndexNotCreated() {
        ElasticsearchStatusException e = expectThrows( ElasticsearchStatusException.class,
            () ->  client()
                .filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(ElasticUser.NAME,
                    new SecureString("password1".toCharArray()))))
                .admin()
                .cluster()
                .prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus()
                .get() );

        assertThat(e.status(), equalTo(RestStatus.UNAUTHORIZED));

        ClusterHealthResponse response = client()
            .filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(ElasticUser.NAME,
                new SecureString("password".toCharArray()))))
            .admin()
            .cluster()
            .prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus()
            .get();

        assertThat(response, notNullValue());
        assertThat(response.isTimedOut(), equalTo(false));
        assertThat(response.status(), equalTo(RestStatus.OK));
        assertThat(response.getStatus(), equalTo(ClusterHealthStatus.GREEN));
        boolean securityIndexCreated = false;
        for (Map.Entry<String, ClusterIndexHealth>  indexEntry: response.getIndices().entrySet()) {
            if (indexEntry.getKey().startsWith(".security")) {
                securityIndexCreated = true;
                break;
            }
        }
        assertFalse(securityIndexCreated);
    }
}
