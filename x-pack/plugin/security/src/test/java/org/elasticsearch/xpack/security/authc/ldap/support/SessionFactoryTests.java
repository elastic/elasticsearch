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
import org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.VerificationMode;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
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
        RealmConfig realmConfig = new RealmConfig(new RealmConfig.RealmIdentifier("ldap", "conn_settings"),
                environment.settings(), environment, new ThreadContext(Settings.EMPTY));
        LDAPConnectionOptions options = SessionFactory.connectionOptions(realmConfig, new SSLService(environment),
                logger);
        assertThat(options.followReferrals(), is(equalTo(true)));
        assertThat(options.allowConcurrentSocketFactoryUse(), is(equalTo(true)));
        assertThat(options.getConnectTimeoutMillis(), is(equalTo(5000)));
        assertThat(options.getResponseTimeoutMillis(), is(equalTo(5000L)));
        assertThat(options.getSSLSocketVerifier(), is(instanceOf(HostNameSSLSocketVerifier.class)));
    }

    public void testConnectionFactoryReturnsCorrectLDAPConnectionOptions() throws Exception {
        final RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier("ldap", "conn_settings");
        final Path pathHome = createTempDir();
        Settings settings = Settings.builder()
                .put(getFullSettingKey(realmId, SessionFactorySettings.TIMEOUT_TCP_CONNECTION_SETTING), "10ms")
                .put(getFullSettingKey(realmId, SessionFactorySettings.HOSTNAME_VERIFICATION_SETTING), "false")
                .put(getFullSettingKey(realmId, SessionFactorySettings.TIMEOUT_TCP_READ_SETTING), "20ms")
                .put(getFullSettingKey(realmId, SessionFactorySettings.FOLLOW_REFERRALS_SETTING), "false")
                .put("path.home", pathHome)
                .build();

        Environment environment = TestEnvironment.newEnvironment(settings);
        RealmConfig realmConfig = new RealmConfig(realmId, settings, environment, new ThreadContext(settings));
        LDAPConnectionOptions options = SessionFactory.connectionOptions(realmConfig, new SSLService(environment), logger);
        assertThat(options.followReferrals(), is(equalTo(false)));
        assertThat(options.allowConcurrentSocketFactoryUse(), is(equalTo(true)));
        assertThat(options.getConnectTimeoutMillis(), is(equalTo(10)));
        assertThat(options.getResponseTimeoutMillis(), is(equalTo(20L)));
        assertThat(options.getSSLSocketVerifier(), is(instanceOf(TrustAllSSLSocketVerifier.class)));
        assertWarnings("the setting [xpack.security.authc.realms.ldap.conn_settings.hostname_verification] has been deprecated and will be "
            + "removed in a future version. use [xpack.security.authc.realms.ldap.conn_settings.ssl.verification_mode] instead");

        settings = Settings.builder()
                .put(getFullSettingKey(realmId, SSLConfigurationSettings.VERIFICATION_MODE_SETTING_REALM), VerificationMode.CERTIFICATE)
                .put("path.home", pathHome)
                .build();
        realmConfig = new RealmConfig(realmId, settings, environment, new ThreadContext(settings));
        options = SessionFactory.connectionOptions(realmConfig, new SSLService(TestEnvironment.newEnvironment(settings)), logger);
        assertThat(options.getSSLSocketVerifier(), is(instanceOf(TrustAllSSLSocketVerifier.class)));

        // Can't run in FIPS with verification_mode none, disable this check instead of duplicating the test case
        if (inFipsJvm() == false) {
            settings = Settings.builder()
                    .put(getFullSettingKey(realmId, SSLConfigurationSettings.VERIFICATION_MODE_SETTING_REALM), VerificationMode.NONE)
                    .put("path.home", pathHome)
                    .build();
            environment = TestEnvironment.newEnvironment(settings);
            realmConfig = new RealmConfig(realmId, settings, environment, new ThreadContext(settings));
            options = SessionFactory.connectionOptions(realmConfig, new SSLService(environment), logger);
            assertThat(options.getSSLSocketVerifier(), is(instanceOf(TrustAllSSLSocketVerifier.class)));
        }

        settings = Settings.builder()
                .put(getFullSettingKey(realmId, SSLConfigurationSettings.VERIFICATION_MODE_SETTING_REALM), VerificationMode.FULL)
                .put("path.home", pathHome)
                .build();
        environment = TestEnvironment.newEnvironment(settings);
        realmConfig = new RealmConfig(realmId, settings, environment, new ThreadContext(settings));
        options = SessionFactory.connectionOptions(realmConfig, new SSLService(environment), logger);
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
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("ldap", "_name");
        final RealmConfig realmConfig = new RealmConfig(realmIdentifier,
                Settings.builder()
                        .put(getFullSettingKey(realmIdentifier, SessionFactorySettings.URLS_SETTING), "ldap://localhost:389")
                        .put(global)
                        .build(),
                TestEnvironment.newEnvironment(global), new ThreadContext(Settings.EMPTY));
        return new SessionFactory(realmConfig, null, threadPool) {

            @Override
            public void session(String user, SecureString password, ActionListener<LdapSession> listener) {
                listener.onResponse(null);
            }
        };
    }
}
