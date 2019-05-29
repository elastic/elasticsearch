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
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.VerificationMode;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory.TlsDeprecationSocketVerifier;
import org.junit.After;
import org.junit.Before;

import java.util.function.Function;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class SessionFactoryTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        threadPool = new TestThreadPool("SessionFactoryTests thread pool");
    }

    @After
    public void shutdown() throws InterruptedException {
        terminate(threadPool);
    }

    public void testConnectionFactoryReturnsCorrectLDAPConnectionOptionsWithDefaultSettings() throws Exception {
        final Environment environment = TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build());
        RealmConfig realmConfig = new RealmConfig("conn settings", Settings.EMPTY, environment.settings(), environment,
                new ThreadContext(Settings.EMPTY));
        LDAPConnectionOptions options = SessionFactory.connectionOptions(realmConfig, new SSLService(environment.settings(), environment),
                logger);
        assertThat(options.followReferrals(), is(equalTo(true)));
        assertThat(options.allowConcurrentSocketFactoryUse(), is(equalTo(true)));
        assertThat(options.getConnectTimeoutMillis(), is(equalTo(5000)));
        assertThat(options.getResponseTimeoutMillis(), is(equalTo(5000L)));
        assertThat(options.getSSLSocketVerifier(), is(instanceOf(TlsDeprecationSocketVerifier.class)));
        final TlsDeprecationSocketVerifier sslSocketVerifier = (TlsDeprecationSocketVerifier) options.getSSLSocketVerifier();
        assertThat(sslSocketVerifier.getDelegate(), is(instanceOf(HostNameSSLSocketVerifier.class)));
    }

    public void testConnectionFactoryReturnsCorrectLDAPConnectionOptions() throws Exception {
        Settings settings = Settings.builder()
                .put(SessionFactorySettings.TIMEOUT_TCP_CONNECTION_SETTING, "10ms")
                .put(SessionFactorySettings.HOSTNAME_VERIFICATION_SETTING, "false")
                .put(SessionFactorySettings.TIMEOUT_TCP_READ_SETTING, "20ms")
                .put(SessionFactorySettings.FOLLOW_REFERRALS_SETTING, "false")
                .putList("ssl.supported_protocols", "TLSv1.2", "TLSv1.1") // disable TLS v1.0 so the verifier doesn't get wrapped
                .build();

        final String realmName = "conn_settings";
        final Function<Settings, Settings> globalSettings = realmSettings -> Settings.builder()
            .put(realmSettings)
            .normalizePrefix(RealmSettings.PREFIX + realmName + ".")
            .put("path.home", createTempDir())
            .build();
        final Environment environment = TestEnvironment.newEnvironment(globalSettings.apply(settings));
        final Function<Settings, SSLService> sslService = realmSettings -> new SSLService(globalSettings.apply(realmSettings), environment);

        final ThreadContext threadContext = new ThreadContext(environment.settings());
        RealmConfig realmConfig = new RealmConfig(realmName, settings, environment.settings(), environment, threadContext);
        LDAPConnectionOptions options = SessionFactory.connectionOptions(realmConfig, sslService.apply(settings), logger);
        assertThat(options.followReferrals(), is(equalTo(false)));
        assertThat(options.allowConcurrentSocketFactoryUse(), is(equalTo(true)));
        assertThat(options.getConnectTimeoutMillis(), is(equalTo(10)));
        assertThat(options.getResponseTimeoutMillis(), is(equalTo(20L)));
        assertThat(options.getSSLSocketVerifier(), is(instanceOf(TrustAllSSLSocketVerifier.class)));
        assertWarnings("the setting [xpack.security.authc.realms." + realmName + ".hostname_verification] has been deprecated" +
            " and will be removed in a future version. use [xpack.security.authc.realms." + realmName + ".ssl.verification_mode] instead");

        settings = Settings.builder()
            .put("ssl.verification_mode", VerificationMode.CERTIFICATE)
            .putList("ssl.supported_protocols", "TLSv1.2", "TLSv1.1") // disable TLS v1.0 so the verifier doesn't get wrapped
            .build();
        realmConfig = new RealmConfig(realmName, settings, globalSettings.apply(settings), environment, threadContext);
        options = SessionFactory.connectionOptions(realmConfig, sslService.apply(settings), logger);
        assertThat(options.getSSLSocketVerifier(), is(instanceOf(TrustAllSSLSocketVerifier.class)));

        // Can't run in FIPS with verification_mode none, disable this check instead of duplicating the test case
        if (inFipsJvm() == false) {
            settings = Settings.builder().put("ssl.verification_mode", VerificationMode.NONE).build();
            realmConfig = new RealmConfig(realmName, settings, environment.settings(), environment, threadContext);
            options = SessionFactory.connectionOptions(realmConfig, sslService.apply(settings), logger);
            assertThat(options.getSSLSocketVerifier(), is(instanceOf(TrustAllSSLSocketVerifier.class)));
        }

        settings = Settings.builder().put("ssl.verification_mode", VerificationMode.FULL).build();
        realmConfig = new RealmConfig(realmName, settings, environment.settings(), environment, threadContext);
        options = SessionFactory.connectionOptions(realmConfig, sslService.apply(settings), logger);
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
                global, TestEnvironment.newEnvironment(global), new ThreadContext(Settings.EMPTY));
        return new SessionFactory(realmConfig, null, threadPool) {

            @Override
            public void session(String user, SecureString password, ActionListener<LdapSession> listener) {
                listener.onResponse(null);
            }
        };
    }
}
