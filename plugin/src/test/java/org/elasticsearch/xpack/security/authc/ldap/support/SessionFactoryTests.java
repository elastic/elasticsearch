/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap.support;

import com.unboundid.ldap.sdk.LDAPConnectionOptions;
import com.unboundid.util.ssl.HostNameSSLSocketVerifier;
import com.unboundid.util.ssl.TrustAllSSLSocketVerifier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ssl.SSLService;
import org.elasticsearch.xpack.ssl.VerificationMode;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class SessionFactoryTests extends ESTestCase {

    public void testConnectionFactoryReturnsCorrectLDAPConnectionOptionsWithDefaultSettings() throws Exception {
        final Environment environment = new Environment(Settings.builder().put("path.home", createTempDir()).build());
        RealmConfig realmConfig = new RealmConfig("conn settings", Settings.EMPTY, environment.settings(), environment,
                new ThreadContext(Settings.EMPTY));
        LDAPConnectionOptions options = SessionFactory.connectionOptions(realmConfig, new SSLService(environment.settings(), environment),
                logger);
        assertThat(options.followReferrals(), is(equalTo(true)));
        assertThat(options.allowConcurrentSocketFactoryUse(), is(equalTo(true)));
        assertThat(options.getConnectTimeoutMillis(), is(equalTo(5000)));
        assertThat(options.getResponseTimeoutMillis(), is(equalTo(5000L)));
        assertThat(options.getSSLSocketVerifier(), is(instanceOf(HostNameSSLSocketVerifier.class)));
    }

    public void testConnectionFactoryReturnsCorrectLDAPConnectionOptions() throws Exception {
        Settings settings = Settings.builder()
                .put(SessionFactory.TIMEOUT_TCP_CONNECTION_SETTING, "10ms")
                .put(SessionFactory.HOSTNAME_VERIFICATION_SETTING, "false")
                .put(SessionFactory.TIMEOUT_TCP_READ_SETTING, "20ms")
                .put(SessionFactory.FOLLOW_REFERRALS_SETTING, "false")
                .build();

        final Environment environment = new Environment(Settings.builder().put("path.home", createTempDir()).build());
        RealmConfig realmConfig = new RealmConfig("conn settings", settings, environment.settings(), environment, new ThreadContext(Settings.EMPTY));
        LDAPConnectionOptions options = SessionFactory.connectionOptions(realmConfig, new SSLService(environment.settings(), environment),
                logger);
        assertThat(options.followReferrals(), is(equalTo(false)));
        assertThat(options.allowConcurrentSocketFactoryUse(), is(equalTo(true)));
        assertThat(options.getConnectTimeoutMillis(), is(equalTo(10)));
        assertThat(options.getResponseTimeoutMillis(), is(equalTo(20L)));
        assertThat(options.getSSLSocketVerifier(), is(instanceOf(TrustAllSSLSocketVerifier.class)));
        assertWarnings("the setting [xpack.security.authc.realms.conn settings.hostname_verification] has been deprecated and will be " +
                "removed in a future version. use [xpack.security.authc.realms.conn settings.ssl.verification_mode] instead");

        settings = Settings.builder().put("ssl.verification_mode", VerificationMode.CERTIFICATE).build();
        realmConfig = new RealmConfig("conn settings", settings, environment.settings(), environment, new ThreadContext(Settings.EMPTY));
        options = SessionFactory.connectionOptions(realmConfig, new SSLService(environment.settings(), environment),
                logger);
        assertThat(options.getSSLSocketVerifier(), is(instanceOf(TrustAllSSLSocketVerifier.class)));

        settings = Settings.builder().put("ssl.verification_mode", VerificationMode.NONE).build();
        realmConfig = new RealmConfig("conn settings", settings, environment.settings(), environment, new ThreadContext(Settings.EMPTY));
        options = SessionFactory.connectionOptions(realmConfig, new SSLService(environment.settings(), environment),
                logger);
        assertThat(options.getSSLSocketVerifier(), is(instanceOf(TrustAllSSLSocketVerifier.class)));

        settings = Settings.builder().put("ssl.verification_mode", VerificationMode.FULL).build();
        realmConfig = new RealmConfig("conn settings", settings, environment.settings(), environment, new ThreadContext(Settings.EMPTY));
        options = SessionFactory.connectionOptions(realmConfig, new SSLService(environment.settings(), environment),
                logger);
        assertThat(options.getSSLSocketVerifier(), is(instanceOf(HostNameSSLSocketVerifier.class)));
    }

    public void testSessionFactoryDoesNotSupportUnauthenticated() {
        assertThat(createSessionFactory().supportsUnauthenticatedSession(), is(false));
    }

    public void testUnauthenticatedSessionThrowsUnsupportedOperationException() throws Exception {
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class,
                () -> createSessionFactory().unauthenticatedSession(randomAlphaOfLength(5), new PlainActionFuture<>()));
        assertThat(e.getMessage(), containsString("unauthenticated sessions"));
    }

    private SessionFactory createSessionFactory() {
        Settings global = Settings.builder().put("path.home", createTempDir()).build();
        final RealmConfig realmConfig = new RealmConfig("_name", Settings.builder().put("url", "ldap://localhost:389").build(),
                global, new ThreadContext(Settings.EMPTY));
        return new SessionFactory(realmConfig, null) {

            @Override
            public void session(String user, SecureString password, ActionListener<LdapSession> listener) {
                listener.onResponse(null);
            }
        };
    }
}
