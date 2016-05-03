/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.activedirectory;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.ldap.LdapSessionFactory;
import org.elasticsearch.shield.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.shield.authc.ldap.support.LdapSession;
import org.elasticsearch.shield.authc.ldap.support.LdapTestCase;
import org.elasticsearch.shield.authc.ldap.support.SessionFactory;
import org.elasticsearch.shield.authc.support.SecuredStringTests;
import org.elasticsearch.shield.ssl.ClientSSLService;
import org.elasticsearch.shield.ssl.SSLConfiguration.Global;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.Network;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.elasticsearch.test.ShieldTestsUtils.assertAuthenticationException;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

@Network
public class ActiveDirectorySessionFactoryTests extends ESTestCase {
    public static final String AD_LDAP_URL = "ldaps://54.213.145.20:636";
    public static final String PASSWORD = "NickFuryHeartsES";
    public static final String AD_DOMAIN = "ad.test.elasticsearch.com";

    private ClientSSLService clientSSLService;
    private Settings globalSettings;
    private boolean useGlobalSSL;

    @Before
    public void initializeSslSocketFactory() throws Exception {
        useGlobalSSL = randomBoolean();
        Path keystore = getDataPath("../ldap/support/ldaptrust.jks");
        /*
         * Prior to each test we reinitialize the socket factory with a new SSLService so that we get a new SSLContext.
         * If we re-use a SSLContext, previously connected sessions can get re-established which breaks hostname
         * verification tests since a re-established connection does not perform hostname verification.
         */
        Settings.Builder builder = Settings.builder().put("path.home", createTempDir());
        if (useGlobalSSL) {
            builder.put("xpack.security.ssl.keystore.path", keystore)
                    .put("xpack.security.ssl.keystore.password", "changeit");
        } else {
            builder.put(Global.AUTO_GENERATE_SSL_SETTING.getKey(), false);
        }
        globalSettings = builder.build();
        Environment environment = new Environment(globalSettings);
        clientSSLService = new ClientSSLService(globalSettings, new Global(globalSettings));
        clientSSLService.setEnvironment(environment);
    }

    @SuppressWarnings("unchecked")
    public void testAdAuth() throws Exception {
        RealmConfig config = new RealmConfig("ad-test", buildAdSettings(AD_LDAP_URL, AD_DOMAIN, false), globalSettings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, clientSSLService).init();

        String userName = "ironman";
        try (LdapSession ldap = sessionFactory.session(userName, SecuredStringTests.build(PASSWORD))) {
            List<String> groups = ldap.groups();
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

    @AwaitsFix(bugUrl = "https://github.com/elasticsearch/elasticsearch-shield/issues/499")
    public void testTcpReadTimeout() throws Exception {
        Settings settings = Settings.builder()
                .put(buildAdSettings(AD_LDAP_URL, AD_DOMAIN, false))
                .put(SessionFactory.HOSTNAME_VERIFICATION_SETTING, false)
                .put(SessionFactory.TIMEOUT_TCP_READ_SETTING, "1ms")
                .build();
        RealmConfig config = new RealmConfig("ad-test", settings, globalSettings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, clientSSLService).init();

        try (LdapSession ldap = sessionFactory.session("ironman", SecuredStringTests.build(PASSWORD))) {
            // In certain cases we may have a successful bind, but a search should take longer and cause a timeout
            ldap.groups();
            fail("The TCP connection should timeout before getting groups back");
        } catch (ElasticsearchSecurityException e) {
            assertAuthenticationException(e);
            assertThat(e.getCause().getMessage(), containsString("A client-side timeout was encountered while waiting"));
        }
    }

    public void testAdAuthAvengers() throws Exception {
        RealmConfig config = new RealmConfig("ad-test", buildAdSettings(AD_LDAP_URL, AD_DOMAIN, false), globalSettings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, clientSSLService).init();

        String[] users = new String[]{"cap", "hawkeye", "hulk", "ironman", "thor", "blackwidow", };
        for(String user: users) {
            try (LdapSession ldap = sessionFactory.session(user, SecuredStringTests.build(PASSWORD))) {
                assertThat("group avenger test for user "+user, ldap.groups(), hasItem(containsString("Avengers")));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testAuthenticate() throws Exception {
        Settings settings = buildAdSettings(AD_LDAP_URL, AD_DOMAIN, "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com",
                LdapSearchScope.ONE_LEVEL, false);
        RealmConfig config = new RealmConfig("ad-test", settings, globalSettings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, clientSSLService).init();

        String userName = "hulk";
        try (LdapSession ldap = sessionFactory.session(userName, SecuredStringTests.build(PASSWORD))) {
            List<String> groups = ldap.groups();

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

    @SuppressWarnings("unchecked")
    public void testAuthenticateBaseUserSearch() throws Exception {
        Settings settings = buildAdSettings(AD_LDAP_URL, AD_DOMAIN, "CN=Bruce Banner, CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com",
                LdapSearchScope.BASE, false);
        RealmConfig config = new RealmConfig("ad-test", settings, globalSettings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, clientSSLService).init();

        String userName = "hulk";
        try (LdapSession ldap = sessionFactory.session(userName, SecuredStringTests.build(PASSWORD))) {
            List<String> groups = ldap.groups();

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

    public void testAuthenticateBaseGroupSearch() throws Exception {
        Settings settings = Settings.builder()
                .put(buildAdSettings(AD_LDAP_URL, AD_DOMAIN, "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com",
                        LdapSearchScope.ONE_LEVEL, false))
                .put(ActiveDirectorySessionFactory.AD_GROUP_SEARCH_BASEDN_SETTING,
                        "CN=Avengers,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com")
                .put(ActiveDirectorySessionFactory.AD_GROUP_SEARCH_SCOPE_SETTING, LdapSearchScope.BASE)
                .build();
        RealmConfig config = new RealmConfig("ad-test", settings, globalSettings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, clientSSLService).init();

        String userName = "hulk";
        try (LdapSession ldap = sessionFactory.session(userName, SecuredStringTests.build(PASSWORD))) {
            List<String> groups = ldap.groups();

            assertThat(groups, hasItem(containsString("Avengers")));
        }
    }

    @SuppressWarnings("unchecked")
    public void testAuthenticateWithUserPrincipalName() throws Exception {
        Settings settings = buildAdSettings(AD_LDAP_URL, AD_DOMAIN, "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com",
                LdapSearchScope.ONE_LEVEL, false);
        RealmConfig config = new RealmConfig("ad-test", settings, globalSettings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, clientSSLService).init();

        //Login with the UserPrincipalName
        String userDN = "CN=Erik Selvig,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
        try (LdapSession ldap = sessionFactory.session("erik.selvig", SecuredStringTests.build(PASSWORD))) {
            List<String> groups = ldap.groups();
            assertThat(ldap.userDn(), is(userDN));
            assertThat(groups, containsInAnyOrder(
                    containsString("Geniuses"),
                    containsString("CN=Users,CN=Builtin"),
                    containsString("Domain Users")));
        }
    }

    public void testAuthenticateWithSAMAccountName() throws Exception {
        Settings settings = buildAdSettings(AD_LDAP_URL, AD_DOMAIN, "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com",
                LdapSearchScope.ONE_LEVEL, false);
        RealmConfig config = new RealmConfig("ad-test", settings, globalSettings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, clientSSLService).init();

        //login with sAMAccountName
        String userDN = "CN=Erik Selvig,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
        try (LdapSession ldap = sessionFactory.session("selvig", SecuredStringTests.build(PASSWORD))) {
            assertThat(ldap.userDn(), is(userDN));

            List<String> groups = ldap.groups();
            assertThat(groups, containsInAnyOrder(
                    containsString("Geniuses"),
                    containsString("CN=Users,CN=Builtin"),
                    containsString("Domain Users")));
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
        RealmConfig config = new RealmConfig("ad-test", settings, globalSettings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, clientSSLService).init();

        //Login with the UserPrincipalName
        try (LdapSession ldap = sessionFactory.session("erik.selvig", SecuredStringTests.build(PASSWORD))) {
            List<String> groups = ldap.groups();
            assertThat(groups, containsInAnyOrder(
                    containsString("CN=Geniuses"),
                    containsString("CN=Domain Users"),
                    containsString("CN=Users,CN=Builtin")));
        }
    }


    @SuppressWarnings("unchecked")
    public void testStandardLdapConnection() throws Exception {
        String groupSearchBase = "DC=ad,DC=test,DC=elasticsearch,DC=com";
        String userTemplate = "CN={0},CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
        Settings settings = LdapTestCase.buildLdapSettings(AD_LDAP_URL, userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE);
        if (useGlobalSSL == false) {
            settings = Settings.builder()
                    .put(settings)
                    .put("ssl.truststore.path", getDataPath("../ldap/support/ldaptrust.jks"))
                    .put("ssl.truststore.password", "changeit")
                    .build();
        }
        RealmConfig config = new RealmConfig("ad-as-ldap-test", settings, globalSettings);
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, clientSSLService).init();

        String user = "Bruce Banner";
        try (LdapSession ldap = sessionFactory.session(user, SecuredStringTests.build(PASSWORD))) {
            List<String> groups = ldap.groups();

            assertThat(groups, containsInAnyOrder(
                    containsString("Avengers"),
                    containsString("SHIELD"),
                    containsString("Geniuses"),
                    containsString("Philanthropists")));
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
        RealmConfig config = new RealmConfig("ad-as-ldap-test", settings, globalSettings);
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, clientSSLService).init();

        String user = "Bruce Banner";
        try (LdapSession ldap = sessionFactory.session(user, SecuredStringTests.build(PASSWORD))) {
            List<String> groups = ldap.groups();

            assertThat(groups, containsInAnyOrder(
                    containsString("Avengers"),
                    containsString("SHIELD"),
                    containsString("Geniuses"),
                    containsString("Philanthropists")));
        }
    }

    public void testAdAuthWithHostnameVerification() throws Exception {
        RealmConfig config = new RealmConfig("ad-test", buildAdSettings(AD_LDAP_URL, AD_DOMAIN, true), globalSettings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, clientSSLService).init();

        String userName = "ironman";
        try (LdapSession ldap = sessionFactory.session(userName, SecuredStringTests.build(PASSWORD))) {
            fail("Test active directory certificate does not have proper hostname/ip address for hostname verification");
        } catch (IOException e) {
            assertThat(e.getMessage(), containsString("failed to connect to any active directory servers"));
        }
    }

    public void testStandardLdapHostnameVerification() throws Exception {
        String groupSearchBase = "DC=ad,DC=test,DC=elasticsearch,DC=com";
        String userTemplate = "CN={0},CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
        Settings settings = Settings.builder()
                .put(LdapTestCase.buildLdapSettings(AD_LDAP_URL, userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put(LdapSessionFactory.HOSTNAME_VERIFICATION_SETTING, true)
                .build();
        RealmConfig config = new RealmConfig("ad-test", settings, globalSettings);
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, clientSSLService).init();

        String user = "Bruce Banner";
        try (LdapSession ldap = sessionFactory.session(user, SecuredStringTests.build(PASSWORD))) {
            fail("Test active directory certificate does not have proper hostname/ip address for hostname verification");
        } catch (IOException e) {
            assertThat(e.getMessage(), containsString("failed to connect to any LDAP servers"));
        }
    }

    Settings buildAdSettings(String ldapUrl, String adDomainName, boolean hostnameVerification) {
        Settings.Builder builder = Settings.builder()
                .put(ActiveDirectorySessionFactory.URLS_SETTING, ldapUrl)
                .put(ActiveDirectorySessionFactory.AD_DOMAIN_NAME_SETTING, adDomainName)
                .put(ActiveDirectorySessionFactory.HOSTNAME_VERIFICATION_SETTING, hostnameVerification);
        if (useGlobalSSL == false) {
            builder.put("ssl.truststore.path", getDataPath("../ldap/support/ldaptrust.jks"))
                    .put("ssl.truststore.password", "changeit");
        }
        return builder.build();
    }

    Settings buildAdSettings(String ldapUrl, String adDomainName, String userSearchDN, LdapSearchScope scope,
                                           boolean hostnameVerification) {
        Settings.Builder builder = Settings.builder()
                .putArray(ActiveDirectorySessionFactory.URLS_SETTING, ldapUrl)
                .put(ActiveDirectorySessionFactory.AD_DOMAIN_NAME_SETTING, adDomainName)
                .put(ActiveDirectorySessionFactory.AD_USER_SEARCH_BASEDN_SETTING, userSearchDN)
                .put(ActiveDirectorySessionFactory.AD_USER_SEARCH_SCOPE_SETTING, scope)
                .put(ActiveDirectorySessionFactory.HOSTNAME_VERIFICATION_SETTING, hostnameVerification);
        if (useGlobalSSL == false) {
            builder.put("ssl.truststore.path", getDataPath("../ldap/support/ldaptrust.jks"))
                    .put("ssl.truststore.password", "changeit");
        }
        return builder.build();
    }
}
