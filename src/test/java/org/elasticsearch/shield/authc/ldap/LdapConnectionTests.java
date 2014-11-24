/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.SecuredStringTests;
import org.elasticsearch.shield.authc.support.ldap.LdapTest;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;

public class LdapConnectionTests extends LdapTest {

    @Test
    public void testBindWithTemplates() {
        String[] ldapUrls = new String[] { ldapUrl() };
        String groupSearchBase = "o=sevenSeas";
        String[] userTemplates = new String[] {
                "cn={0},ou=something,ou=obviously,ou=incorrect,o=sevenSeas",
                "wrongname={0},ou=people,o=sevenSeas",
                "cn={0},ou=people,o=sevenSeas", //this last one should work
        };
        LdapConnectionFactory connectionFactory = new LdapConnectionFactory(
                buildLdapSettings(ldapUrls, userTemplates, groupSearchBase, true));

        String user = "Horatio Hornblower";
        SecuredString userPass = SecuredStringTests.build("pass");

        try (LdapConnection ldap = connectionFactory.open(user, userPass)) {
            String dn = ldap.authenticatedUserDn();
            assertThat(dn, containsString(user));
            //assertThat( attrs.get("uid"), arrayContaining("hhornblo"));
        }
    }

    @Test(expected = LdapException.class)
    public void testBindWithBogusTemplates() {
        String[] ldapUrl = new String[] { ldapUrl() };
        String groupSearchBase = "o=sevenSeas";
        boolean isSubTreeSearch = true;
        String[] userTemplates = new String[] {
                "cn={0},ou=something,ou=obviously,ou=incorrect,o=sevenSeas",
                "wrongname={0},ou=people,o=sevenSeas",
                "asdf={0},ou=people,o=sevenSeas", //none of these should work
        };
        LdapConnectionFactory ldapFac = new LdapConnectionFactory(
                buildLdapSettings(ldapUrl, userTemplates, groupSearchBase, isSubTreeSearch));

        String user = "Horatio Hornblower";
        SecuredString userPass = SecuredStringTests.build("pass");
        try (LdapConnection ldapConnection = ldapFac.open(user, userPass)) {
        }
    }

    @Test
    public void testGroupLookup_Subtree() {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = "cn={0},ou=people,o=sevenSeas";

        LdapConnectionFactory ldapFac = new LdapConnectionFactory(
                buildLdapSettings(ldapUrl(), userTemplate, groupSearchBase, true));

        String user = "Horatio Hornblower";
        SecuredString userPass = SecuredStringTests.build("pass");

        try (LdapConnection ldap = ldapFac.open(user, userPass)) {
            List<String> groups = ldap.getGroupsFromSearch(ldap.authenticatedUserDn());
            assertThat(groups, contains("cn=HMS Lydia,ou=crews,ou=groups,o=sevenSeas"));
        }
    }

    @Test
    public void testGroupLookup_OneLevel() {
        String groupSearchBase = "ou=crews,ou=groups,o=sevenSeas";
        String userTemplate = "cn={0},ou=people,o=sevenSeas";
        LdapConnectionFactory ldapFac = new LdapConnectionFactory(
                buildLdapSettings(ldapUrl(), userTemplate, groupSearchBase, false));

        String user = "Horatio Hornblower";
        try (LdapConnection ldap = ldapFac.open(user, SecuredStringTests.build("pass"))) {
            List<String> groups = ldap.getGroupsFromSearch(ldap.authenticatedUserDn());
            assertThat(groups, contains("cn=HMS Lydia,ou=crews,ou=groups,o=sevenSeas"));
        }
    }
}
