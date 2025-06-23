/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPException;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslVerificationMode;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.fixtures.idp.OpenLdapTestContainer;
import org.elasticsearch.test.fixtures.testcontainers.TestContainersThreadFilter;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.SearchGroupsResolverSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapMetadataResolverSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.ldap.LdapSessionFactory;
import org.elasticsearch.xpack.security.authc.ldap.LdapTestUtils;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapMetadataResolver;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapTestCase;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

@ThreadLeakFilters(filters = { TestContainersThreadFilter.class })
public class OpenLdapTests extends ESTestCase {

    /**
     *
     * ip.es.io is magic that will resolve any IP-like DNS name into the embedded IP
     * This allows us to have an extra DNS name to our local container.
     * This is needed because as of v5.1.2 the LDAP-SDK always trusts
     * connections to loopback addresses if they were made using an IP address,
     * (See {@link com.unboundid.util.ssl.HostNameSSLSocketVerifier}.certificateIncludesHostname)
     * so in order to have a "not-valid-hostname" failure, we need a second
     * hostname that isn't in the certificate's Subj Alt Name list
     */

    @ClassRule
    public static final OpenLdapTestContainer openLdap = new OpenLdapTestContainer();

    public static final String PASSWORD = "NickFuryHeartsES";
    private static final String HAWKEYE_DN = "uid=hawkeye,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
    private static final SecureString PASSWORD_SECURE_STRING = new SecureString(PASSWORD.toCharArray());
    public static final String REALM_NAME = "oldap-test";

    private SSLService sslService;
    private ThreadPool threadPool;
    private Settings globalSettings;

    @Before
    public void init() {
        threadPool = new TestThreadPool("OpenLdapTests thread pool");
    }

    @After
    public void shutdown() throws InterruptedException {
        terminate(threadPool);
    }

    @Override
    public boolean enableWarningsCheck() {
        return false;
    }

    @Before
    public void initializeSslSocketFactory() throws Exception {
        /*
         * Prior to each test we reinitialize the socket factory with a new SSLService so that we get a new SSLContext.
         * If we re-use an SSLContext, previously connected sessions can get re-established which breaks hostname
         * verification tests since a re-established connection does not perform hostname verification.
         */
        MockSecureSettings mockSecureSettings = new MockSecureSettings();
        Settings.Builder builder = Settings.builder().put("path.home", createTempDir());
        // fake realms so ssl will get loaded
        builder.put("xpack.security.authc.realms.ldap.foo.ssl.truststore.path", openLdap.getJavaKeyStorePath());
        mockSecureSettings.setString("xpack.security.authc.realms.ldap.foo.ssl.truststore.secure_password", "changeit");
        builder.put("xpack.security.authc.realms.ldap.foo.ssl.verification_mode", SslVerificationMode.FULL);
        builder.put("xpack.security.authc.realms.ldap." + REALM_NAME + ".ssl.truststore.path", openLdap.getJavaKeyStorePath());
        mockSecureSettings.setString("xpack.security.authc.realms.ldap." + REALM_NAME + ".ssl.truststore.secure_password", "changeit");
        builder.put("xpack.security.authc.realms.ldap." + REALM_NAME + ".ssl.verification_mode", SslVerificationMode.CERTIFICATE);

        builder.put("xpack.security.authc.realms.ldap.vmode_full.ssl.truststore.path", openLdap.getJavaKeyStorePath());
        mockSecureSettings.setString("xpack.security.authc.realms.ldap.vmode_full.ssl.truststore.secure_password", "changeit");
        builder.put("xpack.security.authc.realms.ldap.vmode_full.ssl.verification_mode", SslVerificationMode.FULL);
        globalSettings = builder.setSecureSettings(mockSecureSettings).build();
        Environment environment = TestEnvironment.newEnvironment(globalSettings);
        sslService = new SSLService(environment);
    }

    public void testConnect() throws Exception {
        // openldap does not use cn as naming attributes by default
        String groupSearchBase = "ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        String userTemplate = "uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        final RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier("ldap", "oldap-test");
        RealmConfig config = new RealmConfig(
            realmId,
            buildLdapSettings(realmId, openLdap.getLdapUrl(), userTemplate, groupSearchBase, LdapSearchScope.ONE_LEVEL),
            TestEnvironment.newEnvironment(globalSettings),
            new ThreadContext(Settings.EMPTY)
        );
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, sslService, threadPool);

        String[] users = new String[] { "blackwidow", "cap", "hawkeye", "hulk", "ironman", "thor" };
        for (String user : users) {
            logger.info("testing connect as user [{}]", user);
            try (LdapSession ldap = session(sessionFactory, user, PASSWORD_SECURE_STRING)) {
                assertThat(groups(ldap), hasItem(containsString("Avengers")));
            }
        }
    }

    public void testGroupSearchScopeBase() throws Exception {
        // base search on a groups means that the user can be in just one group

        String groupSearchBase = "cn=Avengers,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        String userTemplate = "uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        final RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier("ldap", REALM_NAME);
        RealmConfig config = new RealmConfig(
            realmId,
            buildLdapSettings(realmId, openLdap.getLdapUrl(), userTemplate, groupSearchBase, LdapSearchScope.BASE),
            TestEnvironment.newEnvironment(globalSettings),
            new ThreadContext(Settings.EMPTY)
        );
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, sslService, threadPool);

        String[] users = new String[] { "blackwidow", "cap", "hawkeye", "hulk", "ironman", "thor" };
        for (String user : users) {
            try (LdapSession ldap = session(sessionFactory, user, PASSWORD_SECURE_STRING)) {
                assertThat(groups(ldap), hasItem(containsString("Avengers")));
            }
        }
    }

    public void testCustomFilter() throws Exception {
        String groupSearchBase = "ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        String userTemplate = "uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        final RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier("ldap", "oldap-test");
        Settings settings = Settings.builder()
            .put(buildLdapSettings(realmId, openLdap.getLdapUrl(), userTemplate, groupSearchBase, LdapSearchScope.ONE_LEVEL))
            .put(getFullSettingKey(realmId.getName(), SearchGroupsResolverSettings.FILTER), "(&(objectclass=posixGroup)(memberUid={0}))")
            .put(getFullSettingKey(realmId.getName(), SearchGroupsResolverSettings.USER_ATTRIBUTE), "uid")
            .build();
        RealmConfig config = new RealmConfig(
            realmId,
            settings,
            TestEnvironment.newEnvironment(globalSettings),
            new ThreadContext(Settings.EMPTY)
        );
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, sslService, threadPool);

        try (LdapSession ldap = session(sessionFactory, "selvig", PASSWORD_SECURE_STRING)) {
            assertThat(groups(ldap), hasItem(containsString("Geniuses")));
        }
    }

    @Network
    public void testStandardLdapConnectionHostnameVerificationFailure() throws Exception {
        // openldap does not use cn as naming attributes by default
        String groupSearchBase = "ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        String userTemplate = "uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        final RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier("ldap", "vmode_full");
        String openLdapEsIoURL = "ldaps://127.0.0.1.ip.es.io:" + openLdap.getDefaultPort();
        Settings settings = Settings.builder()
            // The certificate used in the vagrant box is valid for "localhost", but not for "*.ip.es.io"
            .put(buildLdapSettings(realmId, openLdapEsIoURL, userTemplate, groupSearchBase, LdapSearchScope.ONE_LEVEL))
            .build();
        final Environment env = TestEnvironment.newEnvironment(globalSettings);
        RealmConfig config = new RealmConfig(realmId, settings, env, new ThreadContext(Settings.EMPTY));
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, sslService, threadPool);

        String user = "blackwidow";
        UncategorizedExecutionException e = expectThrows(
            UncategorizedExecutionException.class,
            () -> session(sessionFactory, user, PASSWORD_SECURE_STRING)
        );
        assertThat(e.getCause(), instanceOf(ExecutionException.class));
        assertThat(e.getCause().getCause(), instanceOf(LDAPException.class));
        assertThat(
            e.getCause().getCause().getMessage(),
            anyOf(containsString("Hostname verification failed"), containsString("peer not authenticated"))
        );
    }

    public void testStandardLdapConnectionHostnameVerificationSuccess() throws Exception {
        // openldap does not use cn as naming attributes by default
        String groupSearchBase = "ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        String userTemplate = "uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        final RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier("ldap", "vmode_full");
        Settings settings = Settings.builder()
            // The certificate used in the vagrant box is valid for "localhost" (but not for "*.ip.es.io")
            .put(buildLdapSettings(realmId, openLdap.getLdapUrl(), userTemplate, groupSearchBase, LdapSearchScope.ONE_LEVEL))
            .build();

        RealmConfig config = new RealmConfig(
            realmId,
            settings,
            TestEnvironment.newEnvironment(globalSettings),
            new ThreadContext(Settings.EMPTY)
        );
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, sslService, threadPool);

        final String user = "blackwidow";
        try (LdapSession ldap = session(sessionFactory, user, PASSWORD_SECURE_STRING)) {
            assertThat(ldap, notNullValue());
            assertThat(ldap.userDn(), startsWith("uid=" + user + ","));
        }
    }

    public void testResolveSingleValuedAttributeFromConnection() throws Exception {
        final RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier("ldap", "oldap-test");
        final Settings settings = Settings.builder()
            .putList(
                getFullSettingKey(realmId.getName(), LdapMetadataResolverSettings.ADDITIONAL_METADATA_SETTING.apply("ldap")),
                "cn",
                "sn",
                "mail"
            )
            .put(getFullSettingKey(realmId, RealmSettings.ORDER_SETTING), 0)
            .build();
        final RealmConfig config = new RealmConfig(
            realmId,
            settings,
            TestEnvironment.newEnvironment(globalSettings),
            new ThreadContext(Settings.EMPTY)
        );
        LdapMetadataResolver resolver = new LdapMetadataResolver(config, true);
        try (LDAPConnection ldapConnection = setupOpenLdapConnection()) {
            final Map<String, Object> map = resolve(ldapConnection, resolver);
            assertThat(map.size(), equalTo(3));
            assertThat(map.get("cn"), equalTo("Clint Barton"));
            assertThat(map.get("sn"), equalTo("Clint Barton"));
            assertThat(map.get("mail"), equalTo("hawkeye@oldap.test.elasticsearch.com"));
        }
    }

    public void testResolveMultiValuedAttributeFromConnection() throws Exception {
        final RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier("ldap", "oldap-test");
        final Settings settings = Settings.builder()
            .putList(
                getFullSettingKey(realmId.getName(), LdapMetadataResolverSettings.ADDITIONAL_METADATA_SETTING.apply("ldap")),
                "objectClass"
            )
            .put(getFullSettingKey(realmId, RealmSettings.ORDER_SETTING), 0)
            .build();
        final RealmConfig config = new RealmConfig(
            realmId,
            settings,
            TestEnvironment.newEnvironment(globalSettings),
            new ThreadContext(Settings.EMPTY)
        );
        LdapMetadataResolver resolver = new LdapMetadataResolver(config, true);
        try (LDAPConnection ldapConnection = setupOpenLdapConnection()) {
            final Map<String, Object> map = resolve(ldapConnection, resolver);
            assertThat(map.size(), equalTo(1));
            assertThat(map.get("objectClass"), instanceOf(List.class));
            assertThat((List<?>) map.get("objectClass"), contains("top", "posixAccount", "inetOrgPerson"));
        }
    }

    public void testResolveMissingAttributeFromConnection() throws Exception {
        final RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier("ldap", "oldap-test");
        final Settings settings = Settings.builder()
            .putList(getFullSettingKey(realmId.getName(), LdapMetadataResolverSettings.ADDITIONAL_METADATA_SETTING.apply("ldap")), "alias")
            .put(getFullSettingKey(realmId, RealmSettings.ORDER_SETTING), 0)
            .build();
        final RealmConfig config = new RealmConfig(
            realmId,
            settings,
            TestEnvironment.newEnvironment(globalSettings),
            new ThreadContext(Settings.EMPTY)
        );
        LdapMetadataResolver resolver = new LdapMetadataResolver(config, true);
        try (LDAPConnection ldapConnection = setupOpenLdapConnection()) {
            final Map<String, Object> map = resolve(ldapConnection, resolver);
            assertThat(map.size(), equalTo(0));
        }
    }

    private Settings buildLdapSettings(
        RealmConfig.RealmIdentifier realmId,
        String ldapUrl,
        String userTemplate,
        String groupSearchBase,
        LdapSearchScope scope
    ) {
        final String[] urls = { ldapUrl };
        final String[] templates = { userTemplate };
        Settings.Builder builder = Settings.builder()
            .put(LdapTestCase.buildLdapSettings(realmId, urls, templates, groupSearchBase, scope, null, false));
        builder.put(getFullSettingKey(realmId.getName(), SearchGroupsResolverSettings.USER_ATTRIBUTE), "uid");
        return builder.put(SSLConfigurationSettings.TRUSTSTORE_PATH.realm(realmId).getKey(), openLdap.getJavaKeyStorePath())
            .put(SSLConfigurationSettings.LEGACY_TRUSTSTORE_PASSWORD.realm(realmId).getKey(), "changeit")
            .put(globalSettings)
            .put(getFullSettingKey(realmId, RealmSettings.ORDER_SETTING), 0)
            .build();
    }

    private LdapSession session(SessionFactory factory, String username, SecureString password) {
        PlainActionFuture<LdapSession> future = new PlainActionFuture<>();
        factory.session(username, password, future);
        return future.actionGet();
    }

    private List<String> groups(LdapSession ldapSession) {
        PlainActionFuture<List<String>> future = new PlainActionFuture<>();
        ldapSession.groups(future);
        return future.actionGet();
    }

    private LDAPConnection setupOpenLdapConnection() throws Exception {
        return LdapTestUtils.openConnection(openLdap.getLdapUrl(), HAWKEYE_DN, OpenLdapTests.PASSWORD, openLdap.getJavaKeyStorePath());
    }

    private Map<String, Object> resolve(LDAPConnection connection, LdapMetadataResolver resolver) throws Exception {
        final PlainActionFuture<LdapMetadataResolver.LdapMetadataResult> future = new PlainActionFuture<>();
        resolver.resolve(connection, HAWKEYE_DN, TimeValue.timeValueSeconds(1), logger, null, future);
        return future.get().getMetaData();
    }

    private static String getFromProperty(String port) {
        String key = "test.fixtures.openldap.tcp." + port;
        final String value = System.getProperty(key);
        assertNotNull("Expected the actual value for port " + port + " to be in system property " + key, value);
        return value;
    }
}
