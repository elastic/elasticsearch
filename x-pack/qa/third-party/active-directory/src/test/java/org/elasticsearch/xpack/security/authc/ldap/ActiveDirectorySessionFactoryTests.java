/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.ResultCode;
import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.ActiveDirectorySessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.VerificationMode;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapTestCase;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

@LuceneTestCase.AwaitsFix(bugUrl = "ActiveDirectorySessionFactoryTests")
public class ActiveDirectorySessionFactoryTests extends AbstractActiveDirectoryTestCase {

    private static final String REALM_NAME = "ad-test";
    private static final RealmConfig.RealmIdentifier REALM_ID = new RealmConfig.RealmIdentifier("ad", REALM_NAME);
    private final SecureString SECURED_PASSWORD = new SecureString(PASSWORD);
    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        threadPool = new TestThreadPool("ActiveDirectorySessionFactoryTests thread pool");
    }

    @After
    public void shutdown() throws InterruptedException {
        terminate(threadPool);
    }

    @Override
    public boolean enableWarningsCheck() {
        return false;
    }

    public void testAdAuth() throws Exception {
        RealmConfig config = configureRealm("ad-test", buildAdSettings(AD_LDAP_URL, AD_DOMAIN, false));
        try (ActiveDirectorySessionFactory sessionFactory = getActiveDirectorySessionFactory(config, sslService, threadPool)) {

            String userName = "ironman";
            try (LdapSession ldap = session(sessionFactory, userName, SECURED_PASSWORD)) {
                assertConnectionCanReconnect(ldap.getConnection());
                List<String> groups = groups(ldap);
                assertThat(groups, containsInAnyOrder(
                        containsString("Geniuses"),
                        containsString("Billionaire"),
                        containsString("Playboy"),
                        containsString("Philanthropists"),
                        containsString("Avengers"),
                        containsString("SHIELD"),
                        containsString("CN=Users,CN=Builtin"),
                        containsString("Domain Users"),
                        containsString("Supers")));
            }
        }
    }

    private RealmConfig configureRealm(String name, Settings settings) {
        final Environment env = TestEnvironment.newEnvironment(globalSettings);
        final Settings mergedSettings = Settings.builder()
            .put(settings)
            .normalizePrefix("xpack.security.authc.realms." + name + ".")
            .put(globalSettings)
            .build();
        this.sslService = new SSLService(mergedSettings, env);
        final RealmConfig.RealmIdentifier identifier = new RealmConfig.RealmIdentifier(LdapRealmSettings.AD_TYPE, name);
        return new RealmConfig(identifier, mergedSettings, env, new ThreadContext(globalSettings));
    }

    public void testNetbiosAuth() throws Exception {
        final String adUrl = randomFrom(AD_LDAP_URL, AD_LDAP_GC_URL);
        RealmConfig config = configureRealm("ad-test", buildAdSettings(adUrl, AD_DOMAIN, false));
        try (ActiveDirectorySessionFactory sessionFactory = getActiveDirectorySessionFactory(config, sslService, threadPool)) {

            String userName = "ades\\ironman";
            try (LdapSession ldap = session(sessionFactory, userName, SECURED_PASSWORD)) {
                assertConnectionCanReconnect(ldap.getConnection());
                List<String> groups = groups(ldap);
                assertThat(groups, containsInAnyOrder(
                        containsString("Geniuses"),
                        containsString("Billionaire"),
                        containsString("Playboy"),
                        containsString("Philanthropists"),
                        containsString("Avengers"),
                        containsString("SHIELD"),
                        containsString("CN=Users,CN=Builtin"),
                        containsString("Domain Users"),
                        containsString("Supers")));
            }
        }
    }

    public void testAdAuthAvengers() throws Exception {
        RealmConfig config = configureRealm("ad-test", buildAdSettings(AD_LDAP_URL, AD_DOMAIN, false));
        try (ActiveDirectorySessionFactory sessionFactory = getActiveDirectorySessionFactory(config, sslService, threadPool)) {

            String[] users = new String[]{"cap", "hawkeye", "hulk", "ironman", "thor", "blackwidow"};
            for (String user : users) {
                try (LdapSession ldap = session(sessionFactory, user, SECURED_PASSWORD)) {
                    assertConnectionCanReconnect(ldap.getConnection());
                    assertThat("group avenger test for user " + user, groups(ldap), hasItem(containsString("Avengers")));
                }
            }
        }
    }

    public void testAuthenticate() throws Exception {
        Settings settings = buildAdSettings(REALM_ID, AD_LDAP_URL, AD_DOMAIN, "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com",
                LdapSearchScope.ONE_LEVEL, false);
        RealmConfig config = configureRealm("ad-test", settings);
        try (ActiveDirectorySessionFactory sessionFactory = getActiveDirectorySessionFactory(config, sslService, threadPool)) {

            String userName = "hulk";
            try (LdapSession ldap = session(sessionFactory, userName, SECURED_PASSWORD)) {
                assertConnectionCanReconnect(ldap.getConnection());
                List<String> groups = groups(ldap);

                assertThat(groups, containsInAnyOrder(
                        containsString("Avengers"),
                        containsString("SHIELD"),
                        containsString("Geniuses"),
                        containsString("Philanthropists"),
                        containsString("CN=Users,CN=Builtin"),
                        containsString("Domain Users"),
                        containsString("Supers")));
            }
        }
    }

    public void testAuthenticateBaseUserSearch() throws Exception {
        Settings settings = buildAdSettings(REALM_ID, AD_LDAP_URL, AD_DOMAIN,
                "CN=Bruce Banner, CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com", LdapSearchScope.BASE, false);
        RealmConfig config = configureRealm("ad-test", settings);
        try (ActiveDirectorySessionFactory sessionFactory = getActiveDirectorySessionFactory(config, sslService, threadPool)) {

            String userName = "hulk";
            try (LdapSession ldap = session(sessionFactory, userName, SECURED_PASSWORD)) {
                assertConnectionCanReconnect(ldap.getConnection());
                List<String> groups = groups(ldap);

                assertThat(groups, containsInAnyOrder(
                        containsString("Avengers"),
                        containsString("SHIELD"),
                        containsString("Geniuses"),
                        containsString("Philanthropists"),
                        containsString("CN=Users,CN=Builtin"),
                        containsString("Domain Users"),
                        containsString("Supers")));
            }
        }
    }

    public void testAuthenticateBaseGroupSearch() throws Exception {
        Settings settings = Settings.builder()
                .put(buildAdSettings(REALM_ID, AD_LDAP_URL, AD_DOMAIN, "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com",
                        LdapSearchScope.ONE_LEVEL, false))
                .put(ActiveDirectorySessionFactorySettings.AD_GROUP_SEARCH_BASEDN_SETTING,
                        "CN=Avengers,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com")
                .put(ActiveDirectorySessionFactorySettings.AD_GROUP_SEARCH_SCOPE_SETTING, LdapSearchScope.BASE)
                .build();
        RealmConfig config = configureRealm("ad-test", settings);
        try (ActiveDirectorySessionFactory sessionFactory = getActiveDirectorySessionFactory(config, sslService, threadPool)) {

            String userName = "hulk";
            try (LdapSession ldap = session(sessionFactory, userName, SECURED_PASSWORD)) {
                assertConnectionCanReconnect(ldap.getConnection());
                List<String> groups = groups(ldap);

                assertThat(groups, hasItem(containsString("Avengers")));
            }
        }
    }

    public void testAuthenticateWithUserPrincipalName() throws Exception {
        Settings settings = buildAdSettings(REALM_ID, AD_LDAP_URL, AD_DOMAIN, "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com",
                LdapSearchScope.ONE_LEVEL, false);
        RealmConfig config = configureRealm("ad-test", settings);
        try (ActiveDirectorySessionFactory sessionFactory = getActiveDirectorySessionFactory(config, sslService, threadPool)) {

            //Login with the UserPrincipalName
            String userDN = "CN=Erik Selvig,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
            try (LdapSession ldap = session(sessionFactory, "erik.selvig", SECURED_PASSWORD)) {
                assertConnectionCanReconnect(ldap.getConnection());
                List<String> groups = groups(ldap);
                assertThat(ldap.userDn(), is(userDN));
                assertThat(groups, containsInAnyOrder(
                        containsString("Geniuses"),
                        containsString("CN=Users,CN=Builtin"),
                        containsString("Domain Users")));
            }
        }
    }

    public void testAuthenticateWithSAMAccountName() throws Exception {
        Settings settings = buildAdSettings(REALM_ID, AD_LDAP_URL, AD_DOMAIN, "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com",
                LdapSearchScope.ONE_LEVEL, false);
        RealmConfig config = configureRealm("ad-test", settings);
        try (ActiveDirectorySessionFactory sessionFactory = getActiveDirectorySessionFactory(config, sslService, threadPool)) {

            //login with sAMAccountName
            String userDN = "CN=Erik Selvig,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
            try (LdapSession ldap = session(sessionFactory, "selvig", SECURED_PASSWORD)) {
                assertConnectionCanReconnect(ldap.getConnection());
                assertThat(ldap.userDn(), is(userDN));

                List<String> groups = groups(ldap);
                assertThat(groups, containsInAnyOrder(
                        containsString("Geniuses"),
                        containsString("CN=Users,CN=Builtin"),
                        containsString("Domain Users")));
            }
        }
    }

    public void testCustomUserFilter() throws Exception {
        Settings settings = Settings.builder()
                .put(buildAdSettings(REALM_ID, AD_LDAP_URL, AD_DOMAIN, "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com",
                        LdapSearchScope.SUB_TREE, false))
                .put(getFullSettingKey(REALM_ID.getName(), ActiveDirectorySessionFactorySettings.AD_USER_SEARCH_FILTER_SETTING),
                        "(&(objectclass=user)(userPrincipalName={0}@ad.test.elasticsearch.com))")
                .build();
        RealmConfig config = configureRealm("ad-test", settings);
        try (ActiveDirectorySessionFactory sessionFactory = getActiveDirectorySessionFactory(config, sslService, threadPool)) {

            //Login with the UserPrincipalName
            try (LdapSession ldap = session(sessionFactory, "erik.selvig", SECURED_PASSWORD)) {
                assertConnectionCanReconnect(ldap.getConnection());
                List<String> groups = groups(ldap);
                assertThat(groups, containsInAnyOrder(
                        containsString("CN=Geniuses"),
                        containsString("CN=Domain Users"),
                        containsString("CN=Users,CN=Builtin")));
            }
        }
    }


    public void testStandardLdapConnection() throws Exception {
        String groupSearchBase = "DC=ad,DC=test,DC=elasticsearch,DC=com";
        String userTemplate = "CN={0},CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
        Settings settings = Settings.builder()
            .put(LdapTestCase.buildLdapSettings(
                    new String[]{AD_LDAP_URL},
                    new String[]{userTemplate},
                    groupSearchBase,
                    LdapSearchScope.SUB_TREE,
                    null,
                    true))
            .put("follow_referrals", FOLLOW_REFERRALS)
            .putList("ssl.certificate_authorities", certificatePaths)
            .build();
        RealmConfig config = configureRealm("ad-as-ldap-test", settings);
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, sslService, threadPool);

        String user = "Bruce Banner";
        try (LdapSession ldap = session(sessionFactory, user, SECURED_PASSWORD)) {
            assertConnectionCanReconnect(ldap.getConnection());
            List<String> groups = groups(ldap);

            assertThat(groups, containsInAnyOrder(
                    containsString("Avengers"),
                    containsString("SHIELD"),
                    containsString("Geniuses"),
                    containsString("Philanthropists")));
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/29840")
    public void testHandlingLdapReferralErrors() throws Exception {
        String groupSearchBase = "DC=ad,DC=test,DC=elasticsearch,DC=com";
        String userTemplate = "CN={0},CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
        final boolean ignoreReferralErrors = false;
        final RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier("ad", "ad-as-ldap-test");
        Settings settings = LdapTestCase.buildLdapSettings(realmId,
            new String[]{AD_LDAP_URL},
            new String[]{userTemplate},
            groupSearchBase,
            LdapSearchScope.SUB_TREE,
            null,
            ignoreReferralErrors);
        final Settings.Builder builder = Settings.builder().put(settings).put(globalSettings);
        builder.putList(RealmSettings.realmSslPrefix(realmId) + "certificate_authorities", certificatePaths);
        settings = builder.build();
        RealmConfig config = new RealmConfig(realmId,
                settings, TestEnvironment.newEnvironment(globalSettings),
                new ThreadContext(globalSettings));
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, sslService, threadPool);

        String user = "Bruce Banner";
        try (LdapSession ldap = session(sessionFactory, user, SECURED_PASSWORD)) {
            final UncategorizedExecutionException exception = expectThrows(
                    UncategorizedExecutionException.class,
                    () -> groups(ldap)
            );
            final Throwable cause = exception.getCause();
            assertThat(cause, instanceOf(ExecutionException.class));
            assertThat(cause.getCause(), instanceOf(LDAPException.class));
            final LDAPException ldapException = (LDAPException) cause.getCause();
            assertThat(ldapException.getResultCode(), is(ResultCode.INVALID_CREDENTIALS));
        }
    }

    public void testStandardLdapWithAttributeGroups() throws Exception {
        String userTemplate = "CN={0},CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
        Settings settings = LdapTestCase.buildLdapSettings(new String[]{AD_LDAP_URL}, userTemplate, false);
        settings = Settings.builder()
            .put(settings)
            .putList("ssl.certificate_authorities", certificatePaths)
            .build();
        RealmConfig config = configureRealm("ad-as-ldap-test", settings);
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, sslService, threadPool);

        String user = "Bruce Banner";
        try (LdapSession ldap = session(sessionFactory, user, SECURED_PASSWORD)) {
            assertConnectionCanReconnect(ldap.getConnection());
            List<String> groups = groups(ldap);

            assertThat(groups, containsInAnyOrder(
                    containsString("Avengers"),
                    containsString("SHIELD"),
                    containsString("Geniuses"),
                    containsString("Philanthropists")));
        }
    }

    public void testADLookup() throws Exception {
        RealmConfig config = configureRealm("ad-test", buildAdSettings(AD_LDAP_URL, AD_DOMAIN, false, true));
        try (ActiveDirectorySessionFactory sessionFactory = getActiveDirectorySessionFactory(config, sslService, threadPool)) {

            List<String> users = randomSubsetOf(Arrays.asList("cap", "hawkeye", "hulk", "ironman", "thor", "blackwidow",
                    "cap@ad.test.elasticsearch.com", "hawkeye@ad.test.elasticsearch.com", "hulk@ad.test.elasticsearch.com",
                    "ironman@ad.test.elasticsearch.com", "thor@ad.test.elasticsearch.com", "blackwidow@ad.test.elasticsearch.com",
                    "ADES\\cap", "ADES\\hawkeye", "ADES\\hulk", "ADES\\ironman", "ADES\\thor", "ADES\\blackwidow"));
            for (String user : users) {
                try (LdapSession ldap = unauthenticatedSession(sessionFactory, user)) {
                    assertConnectionCanReconnect(ldap.getConnection());
                    assertNotNull("ldap session was null for user " + user, ldap);
                    assertThat("group avenger test for user " + user, groups(ldap), hasItem(containsString("Avengers")));
                }
            }
        }
    }

    private Settings buildAdSettings(String ldapUrl, String adDomainName, boolean hostnameVerification) {
        return buildAdSettings(ldapUrl, adDomainName, hostnameVerification, randomBoolean());
    }

    private Settings buildAdSettings(String ldapUrl, String adDomainName, boolean hostnameVerification, boolean useBindUser) {
        Settings.Builder builder = Settings.builder()
                .put(getFullSettingKey(REALM_ID, SessionFactorySettings.URLS_SETTING), ldapUrl)
                .put(getFullSettingKey(REALM_NAME, ActiveDirectorySessionFactorySettings.AD_DOMAIN_NAME_SETTING), adDomainName)
                .put(getFullSettingKey(REALM_NAME, ActiveDirectorySessionFactorySettings.AD_LDAP_PORT_SETTING), AD_LDAP_PORT)
                .put(getFullSettingKey(REALM_NAME, ActiveDirectorySessionFactorySettings.AD_LDAPS_PORT_SETTING), AD_LDAPS_PORT)
                .put(getFullSettingKey(REALM_NAME, ActiveDirectorySessionFactorySettings.AD_GC_LDAP_PORT_SETTING), AD_GC_LDAP_PORT)
                .put(getFullSettingKey(REALM_NAME, ActiveDirectorySessionFactorySettings.AD_GC_LDAPS_PORT_SETTING), AD_GC_LDAPS_PORT)
                .put(getFullSettingKey(REALM_ID, SessionFactorySettings.FOLLOW_REFERRALS_SETTING), FOLLOW_REFERRALS);
        if (randomBoolean()) {
            builder.put(getFullSettingKey(REALM_ID, SSLConfigurationSettings.VERIFICATION_MODE_SETTING_REALM),
                    hostnameVerification ? VerificationMode.FULL : VerificationMode.CERTIFICATE);
        } else {
            builder.put(getFullSettingKey(REALM_ID, SessionFactorySettings.HOSTNAME_VERIFICATION_SETTING), hostnameVerification);
        }

        builder.putList(getFullSettingKey(REALM_ID, SSLConfigurationSettings.CAPATH_SETTING_REALM), certificatePaths);

        if (useBindUser) {
            final String user = randomFrom("cap", "hawkeye", "hulk", "ironman", "thor", "blackwidow", "cap@ad.test.elasticsearch.com",
                    "hawkeye@ad.test.elasticsearch.com", "hulk@ad.test.elasticsearch.com", "ironman@ad.test.elasticsearch.com",
                    "thor@ad.test.elasticsearch.com", "blackwidow@ad.test.elasticsearch.com", "ADES\\cap", "ADES\\hawkeye", "ADES\\hulk",
                    "ADES\\ironman", "ADES\\thor", "ADES\\blackwidow", "CN=Bruce Banner,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com");
            final boolean poolingEnabled = randomBoolean();
            builder.put("bind_dn", user)
                    .put("bind_password", PASSWORD)
                    .put("user_search.pool.enabled", poolingEnabled);
            logger.info("using bind user [{}] with pooling enabled [{}]", user, poolingEnabled);
        }
        return builder.build();
    }

    private LdapSession session(SessionFactory factory, String username, SecureString password) {
        PlainActionFuture<LdapSession> future = new PlainActionFuture<>();
        factory.session(username, password, future);
        return future.actionGet();
    }

    private LdapSession unauthenticatedSession(SessionFactory factory, String username) {
        PlainActionFuture<LdapSession> future = new PlainActionFuture<>();
        factory.unauthenticatedSession(username, future);
        return future.actionGet();
    }

    private List<String> groups(LdapSession ldapSession) {
        PlainActionFuture<List<String>> future = new PlainActionFuture<>();
        ldapSession.groups(future);
        return future.actionGet();
    }

    static ActiveDirectorySessionFactory getActiveDirectorySessionFactory(RealmConfig config, SSLService sslService, ThreadPool threadPool)
            throws LDAPException {
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService, threadPool);
        if (sessionFactory.getConnectionPool() != null) {
            // don't use this in production
            // used here to catch bugs that might get masked by an automatic retry
            sessionFactory.getConnectionPool().setRetryFailedOperationsDueToInvalidConnections(false);
        }
        return sessionFactory;
    }
}
