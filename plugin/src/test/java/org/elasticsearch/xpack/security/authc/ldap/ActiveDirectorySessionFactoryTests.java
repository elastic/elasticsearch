/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.ResultCode;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapTestCase;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.xpack.ssl.VerificationMode;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

@Network
public class ActiveDirectorySessionFactoryTests extends AbstractActiveDirectoryIntegTests {

    private final SecureString SECURED_PASSWORD = new SecureString(PASSWORD);

    @Override
    public boolean enableWarningsCheck() {
        return false;
    }

    @SuppressWarnings("unchecked")
    public void testAdAuth() throws Exception {
        RealmConfig config = new RealmConfig("ad-test",
                buildAdSettings(AD_LDAP_URL, AD_DOMAIN, false),
                globalSettings, new ThreadContext(Settings.EMPTY));
        try (ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService)) {

            String userName = "ironman";
            try (LdapSession ldap = session(sessionFactory, userName, SECURED_PASSWORD)) {
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

    public void testNetbiosAuth() throws Exception {
        final String adUrl = randomFrom("ldap://54.213.145.20:3268", "ldaps://54.213.145.20:3269", AD_LDAP_URL);
        RealmConfig config = new RealmConfig("ad-test", buildAdSettings(adUrl, AD_DOMAIN, false), globalSettings,
                new Environment(globalSettings), new ThreadContext(globalSettings));
        try (ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService)) {

            String userName = "ades\\ironman";
            try (LdapSession ldap = session(sessionFactory, userName, SECURED_PASSWORD)) {
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

    @AwaitsFix(bugUrl = "https://github.com/elastic/x-plugins/issues/2849")
    public void testTcpReadTimeout() throws Exception {
        Settings settings = Settings.builder()
                .put(buildAdSettings(AD_LDAP_URL, AD_DOMAIN, false))
                .put("group_search.filter", "(objectClass=*)")
                .put("ssl.verification_mode", VerificationMode.CERTIFICATE)
                .put(SessionFactory.TIMEOUT_TCP_READ_SETTING, "1ms")
                .build();
        RealmConfig config =
                new RealmConfig("ad-test", settings, globalSettings, new Environment(globalSettings), new ThreadContext(globalSettings));
        try (ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService)) {

            PlainActionFuture<List<String>> groups = new PlainActionFuture<>();
            session(sessionFactory, "ironman", SECURED_PASSWORD).groups(groups);
            LDAPException expected = expectThrows(LDAPException.class, groups::actionGet);
            assertThat(expected.getMessage(), containsString("A client-side timeout was encountered while waiting"));
        }
    }

    public void testAdAuthAvengers() throws Exception {
        RealmConfig config = new RealmConfig("ad-test", buildAdSettings(AD_LDAP_URL, AD_DOMAIN, false), globalSettings,
                new Environment(globalSettings), new ThreadContext(globalSettings));
        try (ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService)) {

            String[] users = new String[]{"cap", "hawkeye", "hulk", "ironman", "thor", "blackwidow"};
            for (String user : users) {
                try (LdapSession ldap = session(sessionFactory, user, SECURED_PASSWORD)) {
                    assertThat("group avenger test for user " + user, groups(ldap), hasItem(containsString("Avengers")));
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testAuthenticate() throws Exception {
        Settings settings = buildAdSettings(AD_LDAP_URL, AD_DOMAIN, "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com",
                LdapSearchScope.ONE_LEVEL, false);
        RealmConfig config =
                new RealmConfig("ad-test", settings, globalSettings, new Environment(globalSettings), new ThreadContext(globalSettings));
        try (ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService)) {

            String userName = "hulk";
            try (LdapSession ldap = session(sessionFactory, userName, SECURED_PASSWORD)) {
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

    @SuppressWarnings("unchecked")
    public void testAuthenticateBaseUserSearch() throws Exception {
        Settings settings = buildAdSettings(AD_LDAP_URL, AD_DOMAIN, "CN=Bruce Banner, CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com",
                LdapSearchScope.BASE, false);
        RealmConfig config = new RealmConfig("ad-test", settings, globalSettings, new Environment(globalSettings),
                new ThreadContext(globalSettings));
        try (ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService)) {

            String userName = "hulk";
            try (LdapSession ldap = session(sessionFactory, userName, SECURED_PASSWORD)) {
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
                .put(buildAdSettings(AD_LDAP_URL, AD_DOMAIN, "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com",
                        LdapSearchScope.ONE_LEVEL, false))
                .put(ActiveDirectorySessionFactory.AD_GROUP_SEARCH_BASEDN_SETTING,
                        "CN=Avengers,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com")
                .put(ActiveDirectorySessionFactory.AD_GROUP_SEARCH_SCOPE_SETTING, LdapSearchScope.BASE)
                .build();
        RealmConfig config =
                new RealmConfig("ad-test", settings, globalSettings, new Environment(globalSettings), new ThreadContext(globalSettings));
        try (ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService)) {

            String userName = "hulk";
            try (LdapSession ldap = session(sessionFactory, userName, SECURED_PASSWORD)) {
                List<String> groups = groups(ldap);

                assertThat(groups, hasItem(containsString("Avengers")));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testAuthenticateWithUserPrincipalName() throws Exception {
        Settings settings = buildAdSettings(AD_LDAP_URL, AD_DOMAIN, "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com",
                LdapSearchScope.ONE_LEVEL, false);
        RealmConfig config =
                new RealmConfig("ad-test", settings, globalSettings, new Environment(globalSettings), new ThreadContext(globalSettings));
        try (ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService)) {

            //Login with the UserPrincipalName
            String userDN = "CN=Erik Selvig,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
            try (LdapSession ldap = session(sessionFactory, "erik.selvig", SECURED_PASSWORD)) {
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
        Settings settings = buildAdSettings(AD_LDAP_URL, AD_DOMAIN, "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com",
                LdapSearchScope.ONE_LEVEL, false);
        RealmConfig config =
                new RealmConfig("ad-test", settings, globalSettings, new Environment(globalSettings), new ThreadContext(globalSettings));
        try (ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService)) {

            //login with sAMAccountName
            String userDN = "CN=Erik Selvig,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
            try (LdapSession ldap = session(sessionFactory, "selvig", SECURED_PASSWORD)) {
                assertThat(ldap.userDn(), is(userDN));

                List<String> groups = groups(ldap);
                assertThat(groups, containsInAnyOrder(
                        containsString("Geniuses"),
                        containsString("CN=Users,CN=Builtin"),
                        containsString("Domain Users")));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testCustomUserFilter() throws Exception {
        Settings settings = Settings.builder()
                .put(buildAdSettings(AD_LDAP_URL, AD_DOMAIN, "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com",
                        LdapSearchScope.SUB_TREE, false))
                .put(ActiveDirectorySessionFactory.AD_USER_SEARCH_FILTER_SETTING,
                        "(&(objectclass=user)(userPrincipalName={0}@ad.test.elasticsearch.com))")
                .build();
        RealmConfig config =
                new RealmConfig("ad-test", settings, globalSettings, new Environment(globalSettings), new ThreadContext(globalSettings));
        try (ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService)) {

            //Login with the UserPrincipalName
            try (LdapSession ldap = session(sessionFactory, "erik.selvig", SECURED_PASSWORD)) {
                List<String> groups = groups(ldap);
                assertThat(groups, containsInAnyOrder(
                        containsString("CN=Geniuses"),
                        containsString("CN=Domain Users"),
                        containsString("CN=Users,CN=Builtin")));
            }
        }
    }


    @SuppressWarnings("unchecked")
    public void testStandardLdapConnection() throws Exception {
        String groupSearchBase = "DC=ad,DC=test,DC=elasticsearch,DC=com";
        String userTemplate = "CN={0},CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
        Settings settings = LdapTestCase.buildLdapSettings(
                new String[] { AD_LDAP_URL },
                new String[] { userTemplate },
                groupSearchBase,
                LdapSearchScope.SUB_TREE,
                null,
                true);
        if (useGlobalSSL == false) {
            settings = Settings.builder()
                    .put(settings)
                    .put("ssl.truststore.path", getDataPath("../ldap/support/ldaptrust.jks"))
                    .put("ssl.truststore.password", "changeit")
                    .build();
        }
        RealmConfig config = new RealmConfig("ad-as-ldap-test", settings, globalSettings, new Environment(globalSettings),
                new ThreadContext(globalSettings));
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, sslService);

        String user = "Bruce Banner";
        try (LdapSession ldap = session(sessionFactory, user, SECURED_PASSWORD)) {
            List<String> groups = groups(ldap);

            assertThat(groups, containsInAnyOrder(
                    containsString("Avengers"),
                    containsString("SHIELD"),
                    containsString("Geniuses"),
                    containsString("Philanthropists")));
        }
    }

    @SuppressWarnings("unchecked")
    public void testHandlingLdapReferralErrors() throws Exception {
        String groupSearchBase = "DC=ad,DC=test,DC=elasticsearch,DC=com";
        String userTemplate = "CN={0},CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
        final boolean ignoreReferralErrors = false;
        Settings settings = LdapTestCase.buildLdapSettings(
                new String[] { AD_LDAP_URL },
                new String[] { userTemplate },
                groupSearchBase,
                LdapSearchScope.SUB_TREE,
                null,
                ignoreReferralErrors);
        if (useGlobalSSL == false) {
            settings = Settings.builder()
                    .put(settings)
                    .put("ssl.truststore.path", getDataPath("../ldap/support/ldaptrust.jks"))
                    .put("ssl.truststore.password", "changeit")
                    .build();
        }
        RealmConfig config = new RealmConfig("ad-as-ldap-test", settings, globalSettings, new Environment(globalSettings),
                new ThreadContext(globalSettings));
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, sslService);

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

    @SuppressWarnings("unchecked")
    public void testStandardLdapWithAttributeGroups() throws Exception {
        String userTemplate = "CN={0},CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
        Settings settings = LdapTestCase.buildLdapSettings(new String[] { AD_LDAP_URL }, userTemplate, false);
        if (useGlobalSSL == false) {
            settings = Settings.builder()
                    .put(settings)
                    .put("ssl.truststore.path", getDataPath("../ldap/support/ldaptrust.jks"))
                    .put("ssl.truststore.password", "changeit")
                    .build();
        }
        RealmConfig config = new RealmConfig("ad-as-ldap-test", settings, globalSettings, new Environment(globalSettings),
                new ThreadContext(globalSettings));
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, sslService);

        String user = "Bruce Banner";
        try (LdapSession ldap = session(sessionFactory, user, SECURED_PASSWORD)) {
            List<String> groups = groups(ldap);

            assertThat(groups, containsInAnyOrder(
                    containsString("Avengers"),
                    containsString("SHIELD"),
                    containsString("Geniuses"),
                    containsString("Philanthropists")));
        }
    }

    public void testAdAuthWithHostnameVerification() throws Exception {
        RealmConfig config = new RealmConfig("ad-test", buildAdSettings(AD_LDAP_URL, AD_DOMAIN, true), globalSettings,
                new Environment(globalSettings), new ThreadContext(globalSettings));
        try (ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService)) {

            String userName = "ironman";
            UncategorizedExecutionException e = expectThrows(UncategorizedExecutionException.class,
                    () -> session(sessionFactory, userName, SECURED_PASSWORD));
            assertThat(e.getCause(), instanceOf(ExecutionException.class));
            assertThat(e.getCause().getCause(), instanceOf(LDAPException.class));
            final LDAPException expected = (LDAPException) e.getCause().getCause();
            assertThat(expected.getMessage(),
                    anyOf(containsString("Hostname verification failed"), containsString("peer not authenticated")));
        }
    }

    public void testStandardLdapHostnameVerification() throws Exception {
        String groupSearchBase = "DC=ad,DC=test,DC=elasticsearch,DC=com";
        String userTemplate = "CN={0},CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
        Settings settings = Settings.builder()
                .put(LdapTestCase.buildLdapSettings(AD_LDAP_URL, userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put("ssl.verification_mode", VerificationMode.FULL)
                .build();
        RealmConfig config =
                new RealmConfig("ad-test", settings, globalSettings, new Environment(globalSettings), new ThreadContext(globalSettings));
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, sslService);

        String user = "Bruce Banner";
        UncategorizedExecutionException e = expectThrows(UncategorizedExecutionException.class,
                () -> session(sessionFactory, user, SECURED_PASSWORD));
        assertThat(e.getCause(), instanceOf(ExecutionException.class));
        assertThat(e.getCause().getCause(), instanceOf(LDAPException.class));
        final LDAPException expected = (LDAPException) e.getCause().getCause();
        assertThat(expected.getMessage(), anyOf(containsString("Hostname verification failed"), containsString("peer not authenticated")));
    }

    public void testADLookup() throws Exception {
        RealmConfig config = new RealmConfig("ad-test",
                buildAdSettings(AD_LDAP_URL, AD_DOMAIN, false, true),
                globalSettings, new ThreadContext(Settings.EMPTY));
        try (ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService)) {

            List<String> users = randomSubsetOf(Arrays.asList("cap", "hawkeye", "hulk", "ironman", "thor", "blackwidow",
                    "cap@ad.test.elasticsearch.com", "hawkeye@ad.test.elasticsearch.com", "hulk@ad.test.elasticsearch.com",
                    "ironman@ad.test.elasticsearch.com", "thor@ad.test.elasticsearch.com", "blackwidow@ad.test.elasticsearch.com",
                    "ADES\\cap", "ADES\\hawkeye", "ADES\\hulk", "ADES\\ironman", "ADES\\thor", "ADES\\blackwidow"));
            for (String user : users) {
                try (LdapSession ldap = unauthenticatedSession(sessionFactory, user)) {
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
                .put(ActiveDirectorySessionFactory.URLS_SETTING, ldapUrl)
                .put(ActiveDirectorySessionFactory.AD_DOMAIN_NAME_SETTING, adDomainName);
        if (randomBoolean()) {
            builder.put("ssl.verification_mode", hostnameVerification ? VerificationMode.FULL : VerificationMode.CERTIFICATE);
        } else {
            builder.put(ActiveDirectorySessionFactory.HOSTNAME_VERIFICATION_SETTING, hostnameVerification);
        }

        if (useGlobalSSL == false) {
            builder.put("ssl.truststore.path", getDataPath("../ldap/support/ldaptrust.jks"))
                    .put("ssl.truststore.password", "changeit");
        }

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
}
