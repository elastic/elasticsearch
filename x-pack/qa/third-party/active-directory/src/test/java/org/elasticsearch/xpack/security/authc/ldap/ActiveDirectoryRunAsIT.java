/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateResponse;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;

/**
 * This tests that "run-as" works on LDAP/AD realms
 */
public class ActiveDirectoryRunAsIT extends AbstractAdLdapRealmTestCase {

    private static boolean useLegacyBindPassword;

    @BeforeClass
    public static void selectRealmConfig() {
        realmConfig = RealmConfig.AD;
        useLegacyBindPassword = randomBoolean();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        useLegacyBindPassword = randomBoolean();
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        switch (realmConfig) {
            case AD:
                builder.put(XPACK_SECURITY_AUTHC_REALMS_AD_EXTERNAL + ".bind_dn", "ironman@ad.test.elasticsearch.com")
                        .put(XPACK_SECURITY_AUTHC_REALMS_AD_EXTERNAL + ".user_search.pool.enabled", false);
                if (useLegacyBindPassword) {
                    builder.put(XPACK_SECURITY_AUTHC_REALMS_AD_EXTERNAL + ".bind_password", ActiveDirectorySessionFactoryTests.PASSWORD);
                } else {
                    SecuritySettingsSource.addSecureSettings(builder, secureSettings -> {
                        secureSettings.setString(XPACK_SECURITY_AUTHC_REALMS_AD_EXTERNAL + ".secure_bind_password",
                                ActiveDirectorySessionFactoryTests.PASSWORD);
                    });
                }
                break;
            default:
                throw new IllegalStateException("Unknown realm config " + realmConfig);
        }
        return builder.build();
    }

    public void testRunAs() throws Exception {
        String avenger = realmConfig.loginWithCommonName ? "Natasha Romanoff" : "blackwidow";
        final AuthenticateRequest request = new AuthenticateRequest(avenger);
        final ActionFuture<AuthenticateResponse> future = runAsClient(avenger).execute(AuthenticateAction.INSTANCE, request);
        final AuthenticateResponse response = future.get(30, TimeUnit.SECONDS);
        assertThat(response.authentication().getUser().principal(), Matchers.equalTo(avenger));
    }

    protected Client runAsClient(String user) {
        final Map<String, String> headers = MapBuilder.<String, String>newMapBuilder()
                .put(BASIC_AUTH_HEADER, UsernamePasswordToken.basicAuthHeaderValue(ElasticUser.NAME, BOOTSTRAP_PASSWORD))
                .put(AuthenticationServiceField.RUN_AS_USER_HEADER, user)
                .map();
        return client().filterWithHeader(headers);
    }

}
