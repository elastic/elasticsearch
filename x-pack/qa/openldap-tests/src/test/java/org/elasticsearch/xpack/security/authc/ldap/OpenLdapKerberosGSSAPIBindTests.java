/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.LDAPException;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.OpenLdapTests;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmConfig.RealmIdentifier;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapUserSearchSessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.ldap.SearchGroupsResolverSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.core.security.authc.support.DnRoleMapperSettings;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.VerificationMode;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapTestCase;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.xpack.security.authc.support.DnRoleMapper;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

import javax.security.auth.login.Configuration;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This tests user bind to LDAP happens over SASL - GSSAPI
 */
public class OpenLdapKerberosGSSAPIBindTests extends ESTestCase {

    private static final String GSSAPI_BIND_PASSWORD = "esadmin";
    private static final String GSSAPI_BIND_USER_PRINCIPAL = "kerb-bind-user@DEV.LOCAL";
    private static final String GSSAPI_BIND_KEYTAB_PRINCIPAL = "kerb-ktab-bind-user@DEV.LOCAL";
    private static final String GSSAPI_BIND_KEYTAB_PATH = "/es-bind.keytab";
    private static final String LDAPCACERT_PATH = "/ca_server.pem";
    private static final String LDAPTRUST_PATH = "/ca.jks";

    public static final String VALID_USER_TEMPLATE = "cn={0},ou=people,o=sevenSeas";
    public static final String VALID_USERNAME = "Thomas Masterman Hardy";
    public static final String PASSWORD = "pass";

    private Settings globalSettings;
    private ThreadPool threadPool;



    private ResourceWatcherService resourceWatcherService;
    private Settings defaultGlobalSettings;
    private SSLService sslService;
    private XPackLicenseState licenseState;

    @Before
    public void init() throws PrivilegedActionException {
        AccessController.doPrivileged(new PrivilegedExceptionAction<Void>() {
            @Override
            @SuppressForbidden(reason = "set or clear system property krb5 debug in ldap-gssapi-bind tests")
            public Void run() throws Exception {
                System.setProperty("java.security.krb5.conf", getDataPath("/krb5.conf").toString());
                // JAAS config needs to be cleared as between tests we will be changing the configuration.
                Configuration.setConfiguration(null);

                System.getProperty("sun.security.krb5.debug", "true");
                //System.getProperty("com.unboundid.ldap.sdk.debug.enabled", "true");
                //System.getProperty("com.unboundid.ldap.sdk.debug.level", "FINEST");
                //System.getProperty("com.unboundid.ldap.sdk.debug.type", "LDAP");
                return null;
            }
        });

        Path caPath = getDataPath(LDAPCACERT_PATH);
        /*
         * Prior to each test we reinitialize the socket factory with a new SSLService so that we get a new SSLContext.
         * If we re-use a SSLContext, previously connected sessions can get re-established which breaks hostname
         * verification tests since a re-established connection does not perform hostname verification.
         */
        globalSettings = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.authc.realms.ldap.oldap-test.ssl.certificate_authorities", caPath)
            .build();
        threadPool = new TestThreadPool("OpenLdapKerberosGSSAPIBindTests");
        resourceWatcherService = new ResourceWatcherService(Settings.EMPTY, threadPool);
        defaultGlobalSettings = Settings.builder().put("path.home", createTempDir()).build();
        sslService = new SSLService(defaultGlobalSettings, TestEnvironment.newEnvironment(defaultGlobalSettings));
        licenseState = mock(XPackLicenseState.class);
        when(licenseState.isAuthorizationRealmAllowed()).thenReturn(true);
    }

    @After
    public void shutdown() {
        resourceWatcherService.stop();
        terminate(threadPool);
    }

    public void testAuthenticateUserWhereBindingHappensUsingGSSAPI() {
        Path truststore = getDataPath(LDAPTRUST_PATH);
        /*
         * Prior to each test we reinitialize the socket factory with a new SSLService so that we get a new SSLContext.
         * If we re-use a SSLContext, previously connected sessions can get re-established which breaks hostname
         * verification tests since a re-established connection does not perform hostname verification.
         */
        MockSecureSettings mockSecureSettings = new MockSecureSettings();
        Settings.Builder builder = Settings.builder().put("path.home", createTempDir());
        // fake realms so ssl will get loaded
        builder.put("xpack.security.authc.realms.ldap.foo.ssl.truststore.path", truststore);
        mockSecureSettings.setString("xpack.security.authc.realms.ldap.foo.ssl.truststore.secure_password", "changeit");
        builder.put("xpack.security.authc.realms.ldap.foo.ssl.verification_mode", VerificationMode.FULL);
        builder.put("xpack.security.authc.realms.ldap.oldap-test.ssl.truststore.path", truststore);
        mockSecureSettings.setString("xpack.security.authc.realms.ldap.oldap-test.ssl.truststore.secure_password", "changeit");
        builder.put("xpack.security.authc.realms.ldap.oldap-test.ssl.verification_mode", VerificationMode.CERTIFICATE);

        builder.put("xpack.security.authc.realms.ldap.vmode_full.ssl.truststore.path", truststore);
        mockSecureSettings.setString("xpack.security.authc.realms.ldap.vmode_full.ssl.truststore.secure_password", "changeit");
        builder.put("xpack.security.authc.realms.ldap.vmode_full.ssl.verification_mode", VerificationMode.FULL);
        globalSettings = builder.setSecureSettings(mockSecureSettings).build();
        Environment environment = TestEnvironment.newEnvironment(globalSettings);
        sslService = new SSLService(globalSettings, environment);

        final RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier("ldap", "oldap-test");

        String userTemplate = "uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        final Settings.Builder realmSettings = commonRealmSettings(realmId, userTemplate);
        realmSettings.put(getFullSettingKey(realmId, DnRoleMapperSettings.USE_UNMAPPED_GROUPS_AS_ROLES_SETTING), true);
        bindCredentialsSettings(true, realmId, realmSettings);
        final Settings settings = realmSettings.put(globalSettings).build();

        RealmConfig config = new RealmConfig(realmId, settings, TestEnvironment.newEnvironment(settings), new ThreadContext(settings));
        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);

        LdapRealm ldap = new LdapRealm(config, ldapFactory, new DnRoleMapper(config, resourceWatcherService),
                threadPool);
        ldap.initialize(Collections.singleton(ldap), licenseState);

        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken("hulk", new SecureString(OpenLdapTests.PASSWORD)), future);
        final AuthenticationResult result = future.actionGet();
        assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
    }

    public void testUserSearchWithGSSAPIBindUsingKeytab() throws Exception {
        final RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier("ldap", "oldap-test");

        final Settings.Builder realmSettings = commonRealmSettings(realmId, null);
        bindCredentialsSettings(true, realmId, realmSettings);
        final Settings settings = realmSettings.put(globalSettings).build();

        verifyBindIsSuccessfulAndUserCanBeSearched(realmId, settings);
    }

    public void testUserSearchWithGSSAPIBindUsingUsernamePassword() throws Exception {
        final RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier("ldap", "oldap-test");

        final Settings.Builder realmSettings = commonRealmSettings(realmId, null);
        bindCredentialsSettings(false, realmId, realmSettings);
        final Settings settings = realmSettings.put(globalSettings).build();

        verifyBindIsSuccessfulAndUserCanBeSearched(realmId, settings);
    }

    private Settings.Builder commonRealmSettings(final RealmConfig.RealmIdentifier realmId, String userDnTemplate) {
        String[] userDnTemplates = (Strings.hasText(userDnTemplate)) ? new String[] { userDnTemplate } : Strings.EMPTY_ARRAY;
        String groupSearchBase = "ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        String userSearchBase = "ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        final Settings.Builder realmSettings = Settings.builder()
                .put(LdapTestCase.buildLdapSettings(realmId, new String[]{OpenLdapTests.OPEN_LDAP_DNS_URL}, userDnTemplates,
                        groupSearchBase, LdapSearchScope.ONE_LEVEL, null, false))
                .put(getFullSettingKey(realmId.getName(), LdapUserSearchSessionFactorySettings.SEARCH_BASE_DN), userSearchBase)
                .put(getFullSettingKey(realmId.getName(), SearchGroupsResolverSettings.USER_ATTRIBUTE), "uid")
                .put(getFullSettingKey(realmId.getName(), LdapUserSearchSessionFactorySettings.POOL_ENABLED), randomBoolean())
                .put(getFullSettingKey(realmId, SSLConfigurationSettings.VERIFICATION_MODE_SETTING_REALM), "full");

        realmSettings.put(getFullSettingKey(realmId, PoolingSessionFactorySettings.BIND_MODE),
                "sasl_gssapi");
        realmSettings.put(getFullSettingKey(realmId, PoolingSessionFactorySettings.SASL_GSSAPI_DEBUG), true);
        return realmSettings;
    }

    private void verifyBindIsSuccessfulAndUserCanBeSearched(final RealmConfig.RealmIdentifier realmId, final Settings settings)
            throws LDAPException {
        RealmConfig config = new RealmConfig(realmId, settings,
                TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));

        SSLService sslService = new SSLService(settings, TestEnvironment.newEnvironment(settings));

        // Verify bind happens and user can be searched
        String[] users = new String[]{"cap", "hawkeye", "hulk", "ironman", "thor"};
        try (LdapUserSearchSessionFactory sessionFactory = new LdapUserSearchSessionFactory(config, sslService, threadPool)) {
            logger.info("# BRC = " + sessionFactory.bindRequestCredentials);

            for (String user : users) {
                //auth
                try (LdapSession ldap = session(sessionFactory, user, new SecureString(OpenLdapTests.PASSWORD))) {
                    assertThat(ldap.userDn(), is(equalTo(new MessageFormat("uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com",
                            Locale.ROOT).format(new Object[]{user}, new StringBuffer(), null).toString())));
                    assertThat(groups(ldap), hasItem(containsString("Avengers")));
                }

                //lookup
                try (LdapSession ldap = unauthenticatedSession(sessionFactory, user)) {
                    assertThat(ldap.userDn(), is(equalTo(new MessageFormat("uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com",
                            Locale.ROOT).format(new Object[]{user}, new StringBuffer(), null).toString())));
                    assertThat(groups(ldap), hasItem(containsString("Avengers")));
                }
            }
        }
    }

    private void bindCredentialsSettings(boolean useKeyTab, RealmIdentifier realmId, Settings.Builder realmSettings) {
        logger.info("# useKeyTab = "+useKeyTab);
        logger.info("# useKeyTab = "+getDataPath(GSSAPI_BIND_KEYTAB_PATH));
        if (useKeyTab) {
            realmSettings.put(getFullSettingKey(realmId, PoolingSessionFactorySettings.SASL_GSSAPI_USE_KEYTAB), useKeyTab);
            realmSettings.put(getFullSettingKey(realmId, PoolingSessionFactorySettings.SASL_GSSAPI_PRINCIPAL),
                    GSSAPI_BIND_KEYTAB_PRINCIPAL);
            realmSettings.put(getFullSettingKey(realmId, PoolingSessionFactorySettings.SASL_GSSAPI_KEYTAB_PATH),
                    getDataPath(GSSAPI_BIND_KEYTAB_PATH));
        } else {
            realmSettings.put(getFullSettingKey(realmId, PoolingSessionFactorySettings.SASL_GSSAPI_USE_KEYTAB), useKeyTab);
            realmSettings.put(getFullSettingKey(realmId, PoolingSessionFactorySettings.SASL_GSSAPI_PRINCIPAL),
                    GSSAPI_BIND_USER_PRINCIPAL);
            final MockSecureSettings secureSettings = new MockSecureSettings();
            secureSettings.setString(getFullSettingKey(realmId, PoolingSessionFactorySettings.SECURE_BIND_PASSWORD), GSSAPI_BIND_PASSWORD);
            realmSettings.setSecureSettings(secureSettings);
        }
    }
    
    private LdapSession session(SessionFactory factory, String username, SecureString password) {
        PlainActionFuture<LdapSession> future = new PlainActionFuture<>();
        factory.session(username, password, future);
        return future.actionGet();
    }

    private List<String> groups(LdapSession ldapSession) {
        Objects.requireNonNull(ldapSession);
        PlainActionFuture<List<String>> future = new PlainActionFuture<>();
        ldapSession.groups(future);
        return future.actionGet();
    }

    private LdapSession unauthenticatedSession(SessionFactory factory, String username) {
        PlainActionFuture<LdapSession> future = new PlainActionFuture<>();
        factory.unauthenticatedSession(username, future);
        return future.actionGet();
    }
}
