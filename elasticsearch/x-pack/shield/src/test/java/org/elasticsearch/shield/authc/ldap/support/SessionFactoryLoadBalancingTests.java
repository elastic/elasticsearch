/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap.support;

import com.unboundid.ldap.sdk.LDAPConnection;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.ssl.ClientSSLService;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Tests that the server sets properly load balance connections without throwing exceptions
 */
public class SessionFactoryLoadBalancingTests extends LdapTestCase {

    public void testRoundRobin() throws Exception {
        TestSessionFactory testSessionFactory = createSessionFactory(LdapLoadBalancing.ROUND_ROBIN);

        final int numberOfIterations = randomIntBetween(1, 5);
        for (int iteration = 0; iteration < numberOfIterations; iteration++) {
            for (int i = 0; i < numberOfLdapServers; i++) {
                LDAPConnection connection = null;
                try {
                    connection = testSessionFactory.getServerSet().getConnection();
                    assertThat(connection.getConnectedPort(), is(ldapServers[i].getListenPort()));
                } finally {
                    if (connection != null) {
                        connection.close();
                    }
                }
            }
        }
    }

    public void testRoundRobinWithFailures() throws Exception {
        assumeTrue("at least one ldap server should be present for this test", ldapServers.length > 1);
        TestSessionFactory testSessionFactory = createSessionFactory(LdapLoadBalancing.ROUND_ROBIN);

        // create a list of ports
        List<Integer> ports = new ArrayList<>(numberOfLdapServers);
        for (int i = 0; i < ldapServers.length; i++) {
            ports.add(ldapServers[i].getListenPort());
        }

        int numberToKill = randomIntBetween(1, numberOfLdapServers - 1);
        for (int i = 0; i < numberToKill; i++) {
            int index = randomIntBetween(0, numberOfLdapServers - 1);
            ports.remove(Integer.valueOf(ldapServers[index].getListenPort()));
            ldapServers[index].shutDown(true);
        }

        final int numberOfIterations = randomIntBetween(1, 5);
        for (int iteration = 0; iteration < numberOfIterations; iteration++) {
            for (Integer port : ports) {
                LDAPConnection connection = null;
                try {
                    connection = testSessionFactory.getServerSet().getConnection();
                    assertThat(connection.getConnectedPort(), is(port));
                } finally {
                    if (connection != null) {
                        connection.close();
                    }
                }
            }
        }
    }

    public void testFailover() throws Exception {
        assumeTrue("at least one ldap server should be present for this test", ldapServers.length > 1);
        TestSessionFactory testSessionFactory = createSessionFactory(LdapLoadBalancing.FAILOVER);

        // first test that there is no round robin stuff going on
        final int firstPort = ldapServers[0].getListenPort();
        for (int i = 0; i < numberOfLdapServers; i++) {
            LDAPConnection connection = null;
            try {
                connection = testSessionFactory.getServerSet().getConnection();
                assertThat(connection.getConnectedPort(), is(firstPort));
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
        }

        List<Integer> stoppedServers = new ArrayList<>();
        // now we should kill some servers including the first one
        int numberToKill = randomIntBetween(1, numberOfLdapServers - 1);
        // always kill the first one, but don't add to the list
        ldapServers[0].shutDown(true);
        stoppedServers.add(0);
        for (int i = 0; i < numberToKill - 1; i++) {
            int index = randomIntBetween(1, numberOfLdapServers - 1);
            ldapServers[index].shutDown(true);
            stoppedServers.add(index);
        }

        int firstNonStoppedPort = -1;
        // now we find the first that isn't stopped
        for (int i = 0; i < numberOfLdapServers; i++) {
            if (stoppedServers.contains(i) == false) {
                firstNonStoppedPort = ldapServers[i].getListenPort();
                break;
            }
        }

        assertThat(firstNonStoppedPort, not(-1));
        final int numberOfIterations = randomIntBetween(1, 5);
        for (int iteration = 0; iteration < numberOfIterations; iteration++) {
            LDAPConnection connection = null;
            try {
                connection = testSessionFactory.getServerSet().getConnection();
                assertThat(connection.getConnectedPort(), is(firstNonStoppedPort));
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
        }
    }

    private TestSessionFactory createSessionFactory(LdapLoadBalancing loadBalancing) throws Exception {
        String groupSearchBase = "cn=HMS Lydia,ou=crews,ou=groups,o=sevenSeas";
        String userTemplate = "cn={0},ou=people,o=sevenSeas";
        Settings settings = buildLdapSettings(ldapUrls(), new String[] { userTemplate }, groupSearchBase,
                LdapSearchScope.SUB_TREE, loadBalancing);
        RealmConfig config = new RealmConfig("test-session-factory", settings, Settings.builder().put("path.home",
                createTempDir()).build());
        return new TestSessionFactory(config, null);
    }

    static class TestSessionFactory extends SessionFactory {

        protected TestSessionFactory(RealmConfig config, ClientSSLService sslService) {
            super(config, sslService);
        }

        @Override
        public LdapSession session(String user, SecuredString password) throws Exception {
            return null;
        }
    }
}
