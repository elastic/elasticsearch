/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.support.SecuredStringTests;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.junit.annotations.Network;
import org.hamcrest.Matchers;
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
    public static String SETTINGS_PREFIX = LdapRealm.class.getPackage().getName().substring("com.elasticsearch.".length()) + '.';

    @BeforeClass
    public static void setTrustStore() throws URISyntaxException {
        LdapSslSocketFactory.init(ImmutableSettings.builder()
                .put(SETTINGS_PREFIX + "truststore", new File(LdapConnectionTests.class.getResource("ldaptrust.jks").toURI()))
                .build());
    }


    @Test
    public void testAdAuth() {
        ActiveDirectoryConnectionFactory connectionFactory = new ActiveDirectoryConnectionFactory(
                buildAdSettings(AD_LDAP_URL, AD_DOMAIN));

        String userName = "ironman";
        try (LdapConnection ldap = connectionFactory.bind(userName, SecuredStringTests.build(PASSWORD))) {
            String userDN = ldap.getAuthenticatedUserDn();

            List<String> groups = ldap.getGroupsFromUserAttrs(userDN);
            assertThat(groups, containsInAnyOrder(
                    containsString("Geniuses"),
                    containsString("Billionaire"),
                    containsString("Playboy"),
                    containsString("Philanthropists"),
                    containsString("Avengers"),
                    containsString("SHIELD")));
        }
    }

    @Test
    public void testAdAuth_avengers() {
        ActiveDirectoryConnectionFactory connectionFactory = new ActiveDirectoryConnectionFactory(
                buildAdSettings(AD_LDAP_URL, AD_DOMAIN));

        String[] users = new String[]{"cap", "hawkeye", "hulk", "ironman", "thor", "blackwidow", };
        for(String user: users) {
            try (LdapConnection ldap = connectionFactory.bind(user, SecuredStringTests.build(PASSWORD))) {
                assertThat("group avenger test for user "+user, ldap.getGroups(), hasItem(Matchers.containsString("Avengers")));
            }
        }
    }

    @Test
    public void testAdAuth_specificUserSearch() {
        ActiveDirectoryConnectionFactory connectionFactory = new ActiveDirectoryConnectionFactory(
                buildAdSettings(AD_LDAP_URL, AD_DOMAIN,
                            "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com"));

        String userName = "hulk";
        try (LdapConnection ldap = connectionFactory.bind(userName, SecuredStringTests.build(PASSWORD))) {
            String userDN = ldap.getAuthenticatedUserDn();

            List<String> groups = ldap.getGroupsFromUserAttrs(userDN);

            assertThat(groups, containsInAnyOrder(
                    containsString("Avengers"),
                    containsString("SHIELD"),
                    containsString("Geniuses"),
                    containsString("Philanthropists")));
        }
    }

    @Test
    public void testAD_standardLdapConnection(){
        String groupSearchBase = "DC=ad,DC=test,DC=elasticsearch,DC=com";
        String userTemplate = "CN={0},CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
        boolean isSubTreeSearch = true;
        StandardLdapConnectionFactory connectionFactory = new StandardLdapConnectionFactory(
                LdapTest.buildLdapSettings(AD_LDAP_URL, userTemplate, groupSearchBase, isSubTreeSearch));

        String user = "Bruce Banner";
        try (LdapConnection ldap = connectionFactory.bind(user, SecuredStringTests.build(PASSWORD))) {
            List<String> groups = ldap.getGroupsFromUserAttrs(ldap.getAuthenticatedUserDn());
            List<String> groups2 = ldap.getGroupsFromSearch(ldap.getAuthenticatedUserDn());

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
               .put(SETTINGS_PREFIX + ActiveDirectoryConnectionFactory.URLS_SETTING, ldapUrl)
               .put(SETTINGS_PREFIX + ActiveDirectoryConnectionFactory.AD_DOMAIN_NAME_SETTING, adDomainName)
               .build();
    }

    public static Settings buildAdSettings(String ldapUrl, String adDomainName, String userSearchDN) {
        return ImmutableSettings.builder()
                .putArray(SETTINGS_PREFIX + ActiveDirectoryConnectionFactory.URLS_SETTING, ldapUrl)
                .put(SETTINGS_PREFIX + ActiveDirectoryConnectionFactory.AD_DOMAIN_NAME_SETTING, adDomainName)
                .put(SETTINGS_PREFIX + ActiveDirectoryConnectionFactory.AD_USER_SEARCH_BASEDN_SETTING, userSearchDN)
                .build();
    }
}
