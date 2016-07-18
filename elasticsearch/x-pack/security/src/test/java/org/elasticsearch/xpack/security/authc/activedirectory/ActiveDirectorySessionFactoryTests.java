/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.activedirectory;

import com.unboundid.ldap.sdk.LDAPException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.ldap.LdapSessionFactory;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapTestCase;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.xpack.security.authc.support.SecuredStringTests;
import org.elasticsearch.test.junit.annotations.Network;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

@Network
public class ActiveDirectorySessionFactoryTests extends AbstractActiveDirectoryIntegTests {

    @SuppressWarnings("unchecked")
    public void testAdAuth() throws Exception {
        RealmConfig config = new RealmConfig("ad-test", buildAdSettings(AD_LDAP_URL, AD_DOMAIN, false), globalSettings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, clientSSLService);

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

    public void testNetbiosAuth() throws Exception {
        final String adUrl = randomFrom("ldap://54.213.145.20:3268", "ldaps://54.213.145.20:3269", AD_LDAP_URL);
        RealmConfig config = new RealmConfig("ad-test", buildAdSettings(adUrl, AD_DOMAIN, false), globalSettings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, clientSSLService);

        String userName = "ades\\ironman";
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

    public void testTcpReadTimeout() throws Exception {
        Settings settings = Settings.builder()
                .put(buildAdSettings(AD_LDAP_URL, AD_DOMAIN, false))
                .put("group_search.filter", "(objectClass=*)")
                .put(SessionFactory.HOSTNAME_VERIFICATION_SETTING, false)
                .put(SessionFactory.TIMEOUT_TCP_READ_SETTING, "1ms")
                .build();
        RealmConfig config = new RealmConfig("ad-test", settings, globalSettings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, clientSSLService);

        LDAPException expected = expectThrows(LDAPException.class,
                () -> sessionFactory.session("ironman", SecuredStringTests.build(PASSWORD)).groups());
        assertThat(expected.getMessage(), containsString("A client-side timeout was encountered while waiting"));
    }

    public void testAdAuthAvengers() throws Exception {
        RealmConfig config = new RealmConfig("ad-test", buildAdSettings(AD_LDAP_URL, AD_DOMAIN, false), globalSettings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, clientSSLService);

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
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, clientSSLService);

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
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, clientSSLService);

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
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, clientSSLService);

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
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, clientSSLService);

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
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, clientSSLService);

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
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, clientSSLService);

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
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, clientSSLService);

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
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, clientSSLService);

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
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, clientSSLService);

        String userName = "ironman";
        LDAPException expected =
                expectThrows(LDAPException.class, () -> sessionFactory.session(userName, SecuredStringTests.build(PASSWORD)));
        assertThat(expected.getMessage(), anyOf(containsString("Hostname verification failed"), containsString("peer not authenticated")));
    }

    public void testStandardLdapHostnameVerification() throws Exception {
        String groupSearchBase = "DC=ad,DC=test,DC=elasticsearch,DC=com";
        String userTemplate = "CN={0},CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
        Settings settings = Settings.builder()
                .put(LdapTestCase.buildLdapSettings(AD_LDAP_URL, userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put(LdapSessionFactory.HOSTNAME_VERIFICATION_SETTING, true)
                .build();
        RealmConfig config = new RealmConfig("ad-test", settings, globalSettings);
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, clientSSLService);

        String user = "Bruce Banner";
        LDAPException expected = expectThrows(LDAPException.class, () -> sessionFactory.session(user, SecuredStringTests.build(PASSWORD)));
        assertThat(expected.getMessage(), anyOf(containsString("Hostname verification failed"), containsString("peer not authenticated")));
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
}
