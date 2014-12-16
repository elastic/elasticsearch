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
import org.elasticsearch.shield.authc.support.ldap.ConnectionFactory;
import org.elasticsearch.shield.authc.support.ldap.LdapTest;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.List;

import static org.hamcrest.Matchers.*;

public class LdapConnectionTests extends LdapTest {

    public void testBindWithTimeout() throws Exception {
        int randomPort = randomIntBetween(49152, 65525); // ephemeral port

        // bind own socket locally to not be dependent on the network
        try(ServerSocket serverSocket = new ServerSocket()) {
            SocketAddress sa = new InetSocketAddress("localhost", randomPort);
            serverSocket.setReuseAddress(true);
            serverSocket.bind(sa);

            String[] ldapUrls = new String[] { "ldap://localhost:" + randomPort };
            String groupSearchBase = "o=sevenSeas";
            String[] userTemplates = new String[] {
                    "cn={0},ou=people,o=sevenSeas",
            };
            Settings settings = ImmutableSettings.builder()
                    .put(buildLdapSettings(ldapUrls, userTemplates, groupSearchBase, true))
                    .put(ConnectionFactory.TIMEOUT_TCP_CONNECTION_SETTING, "1ms") //1 millisecond
                    .build();
            LdapConnectionFactory connectionFactory = new LdapConnectionFactory(settings);
            String user = "Horatio Hornblower";
            SecuredString userPass = SecuredStringTests.build("pass");

            long start = System.currentTimeMillis();

            try (LdapConnection connection = connectionFactory.open(user, userPass)) {
                fail("expected connection timeout error here");
            } catch (Throwable t) {
                long time = System.currentTimeMillis() - start;
                assertThat(time, lessThan(1000l));
                assertThat(t, instanceOf(LdapException.class));
            }
        }
    }

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
