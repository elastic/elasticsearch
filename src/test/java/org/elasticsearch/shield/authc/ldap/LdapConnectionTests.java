/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.SecuredStringTests;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.*;

public class LdapConnectionTests extends LdapTest {

    @Test
    public void testBindWithTemplates() {
        String[] ldapUrls = new String[]{apacheDsRule.getUrl()};
        String groupSearchBase = "o=sevenSeas";
        boolean isSubTreeSearch = true;
        String[] userTemplates = new String[]{
                "cn={0},ou=something,ou=obviously,ou=incorrect,o=sevenSeas",
                "wrongname={0},ou=people,o=sevenSeas",
                "cn={0},ou=people,o=sevenSeas", //this last one should work
        };
        StandardLdapConnectionFactory connectionFactory = new StandardLdapConnectionFactory(
                buildLdapSettings(ldapUrls, userTemplates, groupSearchBase, isSubTreeSearch));

        String user = "Horatio Hornblower";
        SecuredString userPass = SecuredStringTests.build("pass");

        LdapConnection ldap = connectionFactory.bind(user, userPass);
        Map<String, String[]> attrs = ldap.getUserAttrs(ldap.getAuthenticatedUserDn());

        assertThat(attrs, hasKey("uid"));
        assertThat( attrs.get("uid"), arrayContaining("hhornblo"));
    }

    @Test(expected = LdapException.class)
    public void testBindWithBogusTemplates() {
        String[] ldapUrl = new String[]{apacheDsRule.getUrl()};
        String groupSearchBase = "o=sevenSeas";
        boolean isSubTreeSearch = true;
        String[] userTemplates = new String[]{
                "cn={0},ou=something,ou=obviously,ou=incorrect,o=sevenSeas",
                "wrongname={0},ou=people,o=sevenSeas",
                "asdf={0},ou=people,o=sevenSeas", //none of these should work
        };
        StandardLdapConnectionFactory ldapFac = new StandardLdapConnectionFactory(
                buildLdapSettings(ldapUrl, userTemplates, groupSearchBase, isSubTreeSearch));

        String user = "Horatio Hornblower";
        SecuredString userPass = SecuredStringTests.build("pass");
        LdapConnection ldap = ldapFac.bind(user, userPass);
    }

    @Test
    public void testGroupLookup_Subtree() {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = "cn={0},ou=people,o=sevenSeas";

        boolean isSubTreeSearch = true;
        StandardLdapConnectionFactory ldapFac = new StandardLdapConnectionFactory(
                buildLdapSettings(apacheDsRule.getUrl(), userTemplate, groupSearchBase, isSubTreeSearch));

        String user = "Horatio Hornblower";
        SecuredString userPass = SecuredStringTests.build("pass");

        LdapConnection ldap = ldapFac.bind(user, userPass);
        List<String> groups = ldap.getGroupsFromSearch(ldap.getAuthenticatedUserDn());
        assertThat(groups, contains("cn=HMS Lydia,ou=crews,ou=groups,o=sevenSeas"));
    }

    @Test
    public void testGroupLookup_OneLevel() {
        String groupSearchBase = "ou=crews,ou=groups,o=sevenSeas";
        String userTemplate = "cn={0},ou=people,o=sevenSeas";
        boolean isSubTreeSearch = false;
        StandardLdapConnectionFactory ldapFac = new StandardLdapConnectionFactory(
                buildLdapSettings(apacheDsRule.getUrl(), userTemplate, groupSearchBase, isSubTreeSearch));

        String user = "Horatio Hornblower";
        LdapConnection ldap = ldapFac.bind(user, SecuredStringTests.build("pass"));

        List<String> groups = ldap.getGroupsFromSearch(ldap.getAuthenticatedUserDn());
        assertThat(groups, contains("cn=HMS Lydia,ou=crews,ou=groups,o=sevenSeas"));
    }
}
