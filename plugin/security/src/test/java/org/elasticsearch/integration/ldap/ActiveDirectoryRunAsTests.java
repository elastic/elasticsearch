/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration.ldap;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.xpack.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.security.action.user.AuthenticateResponse;
import org.elasticsearch.xpack.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.security.authc.ldap.ActiveDirectorySessionFactoryTests;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.user.ElasticUser;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;

import static org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken.BASIC_AUTH_HEADER;

/**
 * This tests that "run-as" works on LDAP/AD realms
 */
@Network
public class ActiveDirectoryRunAsTests extends AbstractAdLdapRealmTestCase {

    @BeforeClass
    public static void selectRealmConfig() {
        realmConfig = randomFrom(RealmConfig.AD, RealmConfig.AD_SSL);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        switch (realmConfig) {
            case AD:
            case AD_SSL:
                builder.put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".bind_dn", "ironman@ad.test.elasticsearch.com")
                        .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".bind_password", ActiveDirectorySessionFactoryTests.PASSWORD)
                        .put(XPACK_SECURITY_AUTHC_REALMS_EXTERNAL + ".user_search.pool.enabled", false);
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
        assertThat(response.user().principal(), Matchers.equalTo(avenger));
    }

    protected Client runAsClient(String user) {
        final Map<String, String> headers = MapBuilder.<String, String>newMapBuilder()
                .put(BASIC_AUTH_HEADER, UsernamePasswordToken.basicAuthHeaderValue(ElasticUser.NAME, BOOTSTRAP_PASSWORD))
                .put(AuthenticationServiceField.RUN_AS_USER_HEADER, user)
                .map();
        return client().filterWithHeader(headers);
    }

}
