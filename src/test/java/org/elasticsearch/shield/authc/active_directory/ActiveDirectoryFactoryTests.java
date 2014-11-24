/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.active_directory;

import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.ldap.LdapConnection;
import org.elasticsearch.shield.authc.ldap.LdapConnectionFactory;
import org.elasticsearch.shield.authc.ldap.LdapConnectionTests;
import org.elasticsearch.shield.authc.support.SecuredStringTests;
import org.elasticsearch.shield.authc.support.ldap.AbstractLdapConnection;
import org.elasticsearch.shield.authc.support.ldap.LdapSslSocketFactory;
import org.elasticsearch.shield.authc.support.ldap.LdapTest;
import org.elasticsearch.shield.ssl.SSLService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.junit.annotations.Network;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.util.List;

import static org.hamcrest.Matchers.*;

@Network
public class ActiveDirectoryFactoryTests extends ElasticsearchTestCase {

    public static final String AD_LDAP_URL = "ldaps://54.213.145.20:636";
    public static final String PASSWORD = "NickFuryHeartsES";
    public static final String AD_DOMAIN = "ad.test.elasticsearch.com";

    @BeforeClass
    public static void setTrustStore() throws URISyntaxException {
        File filename = new File(LdapConnectionTests.class.getResource("../support/ldap/ldaptrust.jks").toURI()).getAbsoluteFile();
        LdapSslSocketFactory.init(Providers.of(new SSLService(ImmutableSettings.builder()
                .put("shield.ssl.keystore.path", filename)
                .put("shield.ssl.keystore.password", "changeit")
                .build())));
    }

    @AfterClass
    public static void clearTrustStore() {
        LdapSslSocketFactory.clear();
    }

    @Test @SuppressWarnings("unchecked")
    public void testAdAuth() {
        ActiveDirectoryConnectionFactory connectionFactory = new ActiveDirectoryConnectionFactory(
                buildAdSettings(AD_LDAP_URL, AD_DOMAIN));

        String userName = "ironman";
        try (AbstractLdapConnection ldap = connectionFactory.open(userName, SecuredStringTests.build(PASSWORD))) {
            List<String> groups = ldap.groups();
            assertThat(groups, containsInAnyOrder(
                    containsString("Geniuses"),
                    containsString("Billionaire"),
                    containsString("Playboy"),
                    containsString("Philanthropists"),
                    containsString("Avengers"),
                    containsString("SHIELD"),
                    containsString("Users"),
                    containsString("Domain Users"),
                    containsString("Supers")));
        }
    }

    @Test
    public void testAdAuth_avengers() {
        ActiveDirectoryConnectionFactory connectionFactory = new ActiveDirectoryConnectionFactory(
                buildAdSettings(AD_LDAP_URL, AD_DOMAIN));

        String[] users = new String[]{"cap", "hawkeye", "hulk", "ironman", "thor", "blackwidow", };
        for(String user: users) {
            try (AbstractLdapConnection ldap = connectionFactory.open(user, SecuredStringTests.build(PASSWORD))) {
                assertThat("group avenger test for user "+user, ldap.groups(), hasItem(Matchers.containsString("Avengers")));
            }
        }
    }

    @Test @SuppressWarnings("unchecked")
    public void testAdAuth_specificUserSearch() {
        Settings settings = buildAdSettings(AD_LDAP_URL, AD_DOMAIN, "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com");
        ActiveDirectoryConnectionFactory connectionFactory = new ActiveDirectoryConnectionFactory(settings);

        String userName = "hulk";
        try (AbstractLdapConnection ldap = connectionFactory.open(userName, SecuredStringTests.build(PASSWORD))) {
            List<String> groups = ldap.groups();

            assertThat(groups, containsInAnyOrder(
                    containsString("Avengers"),
                    containsString("SHIELD"),
                    containsString("Geniuses"),
                    containsString("Philanthropists"),
                    //containsString("Users"),  Users group is in a different user context
                    containsString("Domain Users"),
                    containsString("Supers")));
        }
    }

    @Test @SuppressWarnings("unchecked")
    public void testAD_standardLdapConnection(){
        String groupSearchBase = "DC=ad,DC=test,DC=elasticsearch,DC=com";
        String userTemplate = "CN={0},CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
        Settings settings = LdapTest.buildLdapSettings(AD_LDAP_URL, userTemplate, groupSearchBase, true);
        LdapConnectionFactory connectionFactory = new LdapConnectionFactory(settings);

        String user = "Bruce Banner";
        try (LdapConnection ldap = connectionFactory.open(user, SecuredStringTests.build(PASSWORD))) {
            List<String> groups = ldap.getGroupsFromUserAttrs(ldap.authenticatedUserDn());
            List<String> groups2 = ldap.getGroupsFromSearch(ldap.authenticatedUserDn());

            assertThat(groups, containsInAnyOrder(
                    containsString("Avengers"),
                    containsString("SHIELD"),
                    containsString("Geniuses"),
                    containsString("Philanthropists")));

            assertThat(groups2, containsInAnyOrder(
                    containsString("Avengers"),
                    containsString("SHIELD"),
                    containsString("Geniuses"),
                    containsString("Philanthropists")));
        }
    }

    public static Settings buildAdSettings(String ldapUrl, String adDomainName) {
       return ImmutableSettings.builder()
               .put(ActiveDirectoryConnectionFactory.URLS_SETTING, ldapUrl)
               .put(ActiveDirectoryConnectionFactory.AD_DOMAIN_NAME_SETTING, adDomainName)
               .build();
    }

    public static Settings buildAdSettings(String ldapUrl, String adDomainName, String userSearchDN) {
        return ImmutableSettings.builder()
                .putArray(ActiveDirectoryConnectionFactory.URLS_SETTING, ldapUrl)
                .put(ActiveDirectoryConnectionFactory.AD_DOMAIN_NAME_SETTING, adDomainName)
                .put(ActiveDirectoryConnectionFactory.AD_USER_SEARCH_BASEDN_SETTING, userSearchDN)
                .build();
    }
}
