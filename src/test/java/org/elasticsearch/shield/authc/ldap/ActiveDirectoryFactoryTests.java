/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;

public class ActiveDirectoryFactoryTests extends ElasticsearchTestCase {
    public static final String OPEN_LDAP_URL = "ldap://ad.es.com:389";
    public static final String AD_LDAP_URL = "ldap://54.213.145.20:389";
    public static final String PASSWORD = "4joD8LmWcrEfRa&p";

    public static String SETTINGS_PREFIX = LdapRealm.class.getPackage().getName().substring("com.elasticsearch.".length()) + '.';

    @Ignore
    @Test
    public void testAdAuth() {
        ActiveDirectoryConnectionFactory connectionFactory = new ActiveDirectoryConnectionFactory(
                buildAdSettings(AD_LDAP_URL, "ad.test.elasticsearch.com"));

        String userName = "ironman";

        LdapConnection ldap = connectionFactory.bind(userName, PASSWORD.toCharArray());
        String userDN = ldap.getAuthenticatedUserDn();
        //System.out.println("userPassword check:"+ldap.checkPassword(userDn, userPass));

        List<String> groups = ldap.getGroupsFromUserAttrs(userDN);
        System.out.println("groups: "+groups);

    }

    @Ignore
    @Test
    public void testAD_standardLdapConnection(){
        String groupSearchBase = "dc=ad,dc=test,dc=elasticsearch,dc=com";
        String userTemplate = "cn={0},cn=Users,dc=ad,dc=test,dc=elasticsearch,dc=com";
        boolean isSubTreeSearch = true;
        StandardLdapConnectionFactory connectionFactory = new StandardLdapConnectionFactory(
                LdapConnectionTests.buildLdapSettings(AD_LDAP_URL, userTemplate, groupSearchBase, isSubTreeSearch));

        String user = "Tony Stark";
        LdapConnection ldap = connectionFactory.bind(user, PASSWORD.toCharArray());

        List<String> groups = ldap.getGroupsFromUserAttrs(ldap.getAuthenticatedUserDn());
        List<String> groups2 = ldap.getGroupsFromSearch(ldap.getAuthenticatedUserDn());

        assertThat(groups, containsInAnyOrder(containsString("upchuckers"), containsString("localDistribution")));
        assertThat(groups2, containsInAnyOrder(containsString("upchuckers"), containsString("localDistribution")));
    }


    public static Settings buildAdSettings(String ldapUrl, String adDomainName) {
        return ImmutableSettings.builder()
                .putArray(SETTINGS_PREFIX + ActiveDirectoryConnectionFactory.URLS_SETTING, ldapUrl)
                .put(SETTINGS_PREFIX + ActiveDirectoryConnectionFactory.AD_DOMAIN_NAME_SETTING, adDomainName)
                .build();
    }
}
