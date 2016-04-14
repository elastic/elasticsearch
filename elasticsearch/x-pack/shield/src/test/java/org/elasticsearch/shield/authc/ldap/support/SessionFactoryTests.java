/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap.support;

import com.unboundid.ldap.sdk.LDAPConnectionOptions;
import com.unboundid.util.ssl.HostNameSSLSocketVerifier;
import com.unboundid.util.ssl.TrustAllSSLSocketVerifier;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class SessionFactoryTests extends ESTestCase {
    public void testConnectionFactoryReturnsCorrectLDAPConnectionOptionsWithDefaultSettings() {
        LDAPConnectionOptions options = SessionFactory.connectionOptions(Settings.EMPTY);
        assertThat(options.followReferrals(), is(equalTo(true)));
        assertThat(options.allowConcurrentSocketFactoryUse(), is(equalTo(true)));
        assertThat(options.getConnectTimeoutMillis(), is(equalTo(5000)));
        assertThat(options.getResponseTimeoutMillis(), is(equalTo(5000L)));
        assertThat(options.getSSLSocketVerifier(), is(instanceOf(HostNameSSLSocketVerifier.class)));
    }

    public void testConnectionFactoryReturnsCorrectLDAPConnectionOptions() {
        Settings settings = Settings.builder()
                .put(SessionFactory.TIMEOUT_TCP_CONNECTION_SETTING, "10ms")
                .put(SessionFactory.HOSTNAME_VERIFICATION_SETTING, "false")
                .put(SessionFactory.TIMEOUT_TCP_READ_SETTING, "20ms")
                .put(SessionFactory.FOLLOW_REFERRALS_SETTING, "false")
                .build();
        LDAPConnectionOptions options = SessionFactory.connectionOptions(settings);
        assertThat(options.followReferrals(), is(equalTo(false)));
        assertThat(options.allowConcurrentSocketFactoryUse(), is(equalTo(true)));
        assertThat(options.getConnectTimeoutMillis(), is(equalTo(10)));
        assertThat(options.getResponseTimeoutMillis(), is(equalTo(20L)));
        assertThat(options.getSSLSocketVerifier(), is(instanceOf(TrustAllSSLSocketVerifier.class)));
    }

    public void testSessionFactoryDoesNotSupportUnauthenticated() {
        assertThat(createSessionFactory().supportsUnauthenticatedSession(), is(false));
    }

    public void testUnauthenticatedSessionThrowsUnsupportedOperationException() throws Exception {
        try {
            createSessionFactory().unauthenticatedSession(randomAsciiOfLength(5));
            fail("session factory should throw an unsupported operation exception");
        } catch (UnsupportedOperationException e) {
            // expected...
        }
    }

    private SessionFactory createSessionFactory() {
        Settings global = Settings.builder().put("path.home", createTempDir()).build();
        return new SessionFactory(new RealmConfig("_name", Settings.builder().put("url", "ldap://localhost:389").build(), global), null) {

            @Override
            protected LdapSession getSession(String user, SecuredString password) {
                return null;
            }
        }.init();
    }
}
