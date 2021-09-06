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
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.xpack.core.security.user.ElasticUser;

import java.util.Map;

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
        builder.put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        builder.put("transport.type", "security4");
        builder.put("path.home", customSecuritySettingsSource.nodePath(0));
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
        assertTrue(securityIndexCreated);
        securityIndexExistsAuthenticate();
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

        ClusterHealthResponse response = client()
            .filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue("test_user",
                new SecureString("x-pack-test-password".toCharArray()))))
            .admin()
            .cluster()
            .prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus()
            .get();

        boolean securityIndexCreated = false;
        for (Map.Entry<String, ClusterIndexHealth>  indexEntry: response.getIndices().entrySet()) {
            if (indexEntry.getKey().startsWith(".security")) {
                securityIndexCreated = true;
                break;
            }
        }
        assertFalse(securityIndexCreated);
    }

    public void securityIndexExistsAuthenticate() {
        ClusterHealthResponse response = client()
            .filterWithHeader(singletonMap("Authorization", basicAuthHeaderValue(ElasticUser.NAME,
                new SecureString("password1".toCharArray()))))
            .admin()
            .cluster()
            .prepareHealth()
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
        assertTrue(securityIndexCreated);
    }
}
