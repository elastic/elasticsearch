/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.sdk.BindRequest;
import com.unboundid.ldap.sdk.GetEntryLDAPConnectionPoolHealthCheck;
import com.unboundid.ldap.sdk.LDAPConnectionPool;
import com.unboundid.ldap.sdk.LDAPConnectionPoolHealthCheck;
import com.unboundid.ldap.sdk.LDAPURL;
import com.unboundid.ldap.sdk.SimpleBindRequest;
import com.unboundid.ldap.sdk.SingleServerSet;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapTestCase;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.elasticsearch.xpack.security.authc.support.SecuredStringTests;
import org.elasticsearch.xpack.ssl.SSLService;
import org.elasticsearch.xpack.security.support.NoOpLogger;
import org.elasticsearch.test.junit.annotations.Network;
import org.junit.Before;

import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class LdapUserSearchSessionFactoryTests extends LdapTestCase {

    private SSLService sslService;
    private Settings globalSettings;

    @Before
    public void initializeSslSocketFactory() throws Exception {
        Path keystore = getDataPath("support/ldaptrust.jks");
        Environment env = new Environment(Settings.builder().put("path.home", createTempDir()).build());
        /*
         * Prior to each test we reinitialize the socket factory with a new SSLService so that we get a new SSLContext.
         * If we re-use a SSLContext, previously connected sessions can get re-established which breaks hostname
         * verification tests since a re-established connection does not perform hostname verification.
         */
        globalSettings = Settings.builder()
                .put("path.home", createTempDir())
                .put("xpack.ssl.truststore.path", keystore)
                .put("xpack.ssl.truststore.password", "changeit")
                .build();
        sslService = new SSLService(globalSettings, env);
    }

    public void testSupportsUnauthenticatedSessions() throws Exception {
        RealmConfig config = new RealmConfig("ldap_realm", Settings.builder()
                .put(buildLdapSettings(ldapUrls(), Strings.EMPTY_ARRAY, "", LdapSearchScope.SUB_TREE))
                .put("user_search.base_dn", "")
                .put("bind_dn", "cn=Horatio Hornblower,ou=people,o=sevenSeas")
                .put("bind_password", "pass")
                .put("user_search.attribute", "cn")
                .put("user_search.pool.enabled", randomBoolean())
                .build(), globalSettings);

        LdapUserSearchSessionFactory sessionFactory = new LdapUserSearchSessionFactory(config, sslService);
        try {
            assertThat(sessionFactory.supportsUnauthenticatedSession(), is(true));
        } finally {
            sessionFactory.shutdown();
        }
    }

    public void testUserSearchSubTree() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userSearchBase = "o=sevenSeas";

        RealmConfig config = new RealmConfig("ldap_realm", Settings.builder()
                .put(buildLdapSettings(ldapUrls(), Strings.EMPTY_ARRAY, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put("user_search.base_dn", userSearchBase)
                .put("bind_dn", "cn=Horatio Hornblower,ou=people,o=sevenSeas")
                .put("bind_password", "pass")
                .put("user_search.attribute", "cn")
                .put("user_search.pool.enabled", randomBoolean())
                .build(), globalSettings);

        LdapUserSearchSessionFactory sessionFactory = new LdapUserSearchSessionFactory(config, sslService);

        String user = "William Bush";
        SecuredString userPass = SecuredStringTests.build("pass");

        try {
            // auth
            try (LdapSession ldap = session(sessionFactory, user, userPass)) {
                String dn = ldap.userDn();
                assertThat(dn, containsString(user));
            }

            //lookup
            try (LdapSession ldap = unauthenticatedSession(sessionFactory, user)) {
                String dn = ldap.userDn();
                assertThat(dn, containsString(user));
            }
        } finally {
            sessionFactory.shutdown();
        }
    }

    public void testUserSearchBaseScopeFailsWithWrongBaseDN() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userSearchBase = "o=sevenSeas";

        RealmConfig config = new RealmConfig("ldap_realm", Settings.builder()
                .put(buildLdapSettings(ldapUrls(), Strings.EMPTY_ARRAY, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put("user_search.base_dn", userSearchBase)
                .put("bind_dn", "cn=Horatio Hornblower,ou=people,o=sevenSeas")
                .put("bind_password", "pass")
                .put("user_search.scope", LdapSearchScope.BASE)
                .put("user_search.attribute", "cn")
                .put("user_search.pool.enabled", randomBoolean())
                .build(), globalSettings);

        LdapUserSearchSessionFactory sessionFactory = new LdapUserSearchSessionFactory(config, sslService);

        String user = "William Bush";
        SecuredString userPass = SecuredStringTests.build("pass");

        try {
            assertNull(session(sessionFactory, user, userPass));
            assertNull(unauthenticatedSession(sessionFactory, user));
        } finally {
            sessionFactory.shutdown();
        }
    }

    public void testUserSearchBaseScopePassesWithCorrectBaseDN() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userSearchBase = "cn=William Bush,ou=people,o=sevenSeas";

        RealmConfig config = new RealmConfig("ldap_realm", Settings.builder()
                .put(buildLdapSettings(ldapUrls(), Strings.EMPTY_ARRAY, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put("user_search.base_dn", userSearchBase)
                .put("bind_dn", "cn=Horatio Hornblower,ou=people,o=sevenSeas")
                .put("bind_password", "pass")
                .put("user_search.scope", LdapSearchScope.BASE)
                .put("user_search.attribute", "cn")
                .put("user_search.pool.enabled", randomBoolean())
                .build(), globalSettings);

        LdapUserSearchSessionFactory sessionFactory = new LdapUserSearchSessionFactory(config, sslService);

        String user = "William Bush";
        SecuredString userPass = SecuredStringTests.build("pass");

        try {
            // auth
            try (LdapSession ldap = session(sessionFactory, user, userPass)) {
                String dn = ldap.userDn();
                assertThat(dn, containsString(user));
            }

            //lookup
            try (LdapSession ldap = unauthenticatedSession(sessionFactory, user)) {
                String dn = ldap.userDn();
                assertThat(dn, containsString(user));
            }
        } finally {
            sessionFactory.shutdown();
        }
    }

    public void testUserSearchOneLevelScopeFailsWithWrongBaseDN() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userSearchBase = "o=sevenSeas";

        RealmConfig config = new RealmConfig("ldap_realm", Settings.builder()
                .put(buildLdapSettings(ldapUrls(), Strings.EMPTY_ARRAY, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put("user_search.base_dn", userSearchBase)
                .put("bind_dn", "cn=Horatio Hornblower,ou=people,o=sevenSeas")
                .put("bind_password", "pass")
                .put("user_search.scope", LdapSearchScope.ONE_LEVEL)
                .put("user_search.attribute", "cn")
                .put("user_search.pool.enabled", randomBoolean())
                .build(), globalSettings);

        LdapUserSearchSessionFactory sessionFactory = new LdapUserSearchSessionFactory(config, sslService);

        String user = "William Bush";
        SecuredString userPass = SecuredStringTests.build("pass");

        try {
            assertNull(session(sessionFactory, user, userPass));
            assertNull(unauthenticatedSession(sessionFactory, user));
        } finally {
            sessionFactory.shutdown();
        }
    }

    public void testUserSearchOneLevelScopePassesWithCorrectBaseDN() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userSearchBase = "ou=people,o=sevenSeas";

        RealmConfig config = new RealmConfig("ldap_realm", Settings.builder()
                .put(buildLdapSettings(ldapUrls(), Strings.EMPTY_ARRAY, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put("user_search.base_dn", userSearchBase)
                .put("bind_dn", "cn=Horatio Hornblower,ou=people,o=sevenSeas")
                .put("bind_password", "pass")
                .put("user_search.scope", LdapSearchScope.ONE_LEVEL)
                .put("user_search.attribute", "cn")
                .put("user_search.pool.enabled", randomBoolean())
                .build(), globalSettings);

        LdapUserSearchSessionFactory sessionFactory = new LdapUserSearchSessionFactory(config, sslService);

        String user = "William Bush";
        SecuredString userPass = SecuredStringTests.build("pass");

        try {
            //auth
            try (LdapSession ldap = session(sessionFactory, user, userPass)) {
                String dn = ldap.userDn();
                assertThat(dn, containsString(user));
            }

            //lookup
            try (LdapSession ldap = unauthenticatedSession(sessionFactory, user)) {
                String dn = ldap.userDn();
                assertThat(dn, containsString(user));
            }
        } finally {
            sessionFactory.shutdown();
        }
    }

    public void testUserSearchWithBadAttributeFails() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userSearchBase = "o=sevenSeas";

        RealmConfig config = new RealmConfig("ldap_realm", Settings.builder()
                .put(buildLdapSettings(ldapUrls(), Strings.EMPTY_ARRAY, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put("user_search.base_dn", userSearchBase)
                .put("bind_dn", "cn=Horatio Hornblower,ou=people,o=sevenSeas")
                .put("bind_password", "pass")
                .put("user_search.attribute", "uid1")
                .put("user_search.pool.enabled", randomBoolean())
                .build(), globalSettings);

        LdapUserSearchSessionFactory sessionFactory = new LdapUserSearchSessionFactory(config, sslService);

        String user = "William Bush";
        SecuredString userPass = SecuredStringTests.build("pass");

        try {
            assertNull(session(sessionFactory, user, userPass));
            assertNull(unauthenticatedSession(sessionFactory, user));
        } finally {
            sessionFactory.shutdown();
        }
    }

    public void testUserSearchWithoutAttributePasses() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userSearchBase = "o=sevenSeas";

        RealmConfig config = new RealmConfig("ldap_realm", Settings.builder()
                .put(buildLdapSettings(ldapUrls(), Strings.EMPTY_ARRAY, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put("user_search.base_dn", userSearchBase)
                .put("bind_dn", "cn=Horatio Hornblower,ou=people,o=sevenSeas")
                .put("bind_password", "pass")
                .put("user_search.pool.enabled", randomBoolean())
                .build(), globalSettings);

        LdapUserSearchSessionFactory sessionFactory = new LdapUserSearchSessionFactory(config, sslService);

        String user = "wbush";
        SecuredString userPass = SecuredStringTests.build("pass");

        try {
            //auth
            try (LdapSession ldap = session(sessionFactory, user, userPass)) {
                String dn = ldap.userDn();
                assertThat(dn, containsString("William Bush"));
            }

            //lookup
            try (LdapSession ldap = unauthenticatedSession(sessionFactory, user)) {
                String dn = ldap.userDn();
                assertThat(dn, containsString("William Bush"));
            }
        } finally {
            sessionFactory.shutdown();
        }
    }

    @Network
    public void testUserSearchWithActiveDirectory() throws Exception {
        String groupSearchBase = "DC=ad,DC=test,DC=elasticsearch,DC=com";
        String userSearchBase = "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
        Settings settings = Settings.builder()
                .put(LdapTestCase.buildLdapSettings(new String[] { ActiveDirectorySessionFactoryTests.AD_LDAP_URL }, Strings.EMPTY_ARRAY,
                        groupSearchBase, LdapSearchScope.SUB_TREE))
                .put("user_search.base_dn", userSearchBase)
                .put("bind_dn", "ironman@ad.test.elasticsearch.com")
                .put("bind_password", ActiveDirectorySessionFactoryTests.PASSWORD)
                .put("user_search.attribute", "cn")
                .put("user_search.pool.enabled", randomBoolean())
                .build();
        Settings.Builder builder = Settings.builder()
                .put(globalSettings);
        for (Map.Entry<String, String> entry : settings.getAsMap().entrySet()) {
            builder.put("xpack.security.authc.realms.ldap." + entry.getKey(), entry.getValue());
        }
        Settings fullSettings = builder.build();
        sslService = new SSLService(fullSettings, new Environment(fullSettings));
        RealmConfig config = new RealmConfig("ad-as-ldap-test", settings, globalSettings);
        LdapUserSearchSessionFactory sessionFactory = new LdapUserSearchSessionFactory(config, sslService);

        String user = "Bruce Banner";
        try {
            //auth
            try (LdapSession ldap = session(sessionFactory, user, SecuredStringTests.build(ActiveDirectorySessionFactoryTests.PASSWORD))) {
                List<String> groups = groups(ldap);

                assertThat(groups, containsInAnyOrder(
                        containsString("Avengers"),
                        containsString("SHIELD"),
                        containsString("Geniuses"),
                        containsString("Philanthropists")));
            }

            //lookup
            try (LdapSession ldap = unauthenticatedSession(sessionFactory, user)) {
                List<String> groups = groups(ldap);

                assertThat(groups, containsInAnyOrder(
                        containsString("Avengers"),
                        containsString("SHIELD"),
                        containsString("Geniuses"),
                        containsString("Philanthropists")));
            }
        } finally {
            sessionFactory.shutdown();
        }
    }

    @Network
    public void testUserSearchwithBindUserOpenLDAP() throws Exception {
        String groupSearchBase = "ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        String userSearchBase = "ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        RealmConfig config = new RealmConfig("oldap-test", Settings.builder()
                .put(LdapTestCase.buildLdapSettings(new String[] { OpenLdapTests.OPEN_LDAP_URL }, Strings.EMPTY_ARRAY, groupSearchBase,
                        LdapSearchScope.ONE_LEVEL))
                .put("user_search.base_dn", userSearchBase)
                .put("bind_dn", "uid=blackwidow,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .put("bind_password", OpenLdapTests.PASSWORD)
                .put("user_search.pool.enabled", randomBoolean())
                .build(), globalSettings);
        Settings.Builder builder = Settings.builder()
                .put(globalSettings);
        for (Map.Entry<String, String> entry : config.settings().getAsMap().entrySet()) {
            builder.put("xpack.security.authc.realms.ldap." + entry.getKey(), entry.getValue());
        }
        Settings settings = builder.build();
        sslService = new SSLService(settings, new Environment(settings));
        LdapUserSearchSessionFactory sessionFactory = new LdapUserSearchSessionFactory(config, sslService);

        String[] users = new String[] { "cap", "hawkeye", "hulk", "ironman", "thor" };
        try {
            for (String user : users) {
                //auth
                try (LdapSession ldap = session(sessionFactory, user, SecuredStringTests.build(OpenLdapTests.PASSWORD))) {
                    assertThat(ldap.userDn(), is(equalTo(new MessageFormat("uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com",
                            Locale.ROOT).format(new Object[]{user}, new StringBuffer(), null).toString())));
                    assertThat(groups(ldap), hasItem(containsString("Avengers")));
                }

                //lookup
                try (LdapSession ldap = unauthenticatedSession(sessionFactory, user)) {
                    assertThat(ldap.userDn(), is(equalTo(new MessageFormat("uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com",
                            Locale.ROOT).format(new Object[]{user}, new StringBuffer(), null).toString())));
                    assertThat(groups(ldap), hasItem(containsString("Avengers")));
                }
            }
        } finally {
            sessionFactory.shutdown();
        }
    }

    public void testConnectionPoolDefaultSettings() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userSearchBase = "o=sevenSeas";
        RealmConfig config = new RealmConfig("ldap_realm", Settings.builder()
                .put(buildLdapSettings(ldapUrls(), Strings.EMPTY_ARRAY, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put("user_search.base_dn", userSearchBase)
                .put("bind_dn", "cn=Horatio Hornblower,ou=people,o=sevenSeas")
                .put("bind_password", "pass")
                .build(), globalSettings);

        LDAPConnectionPool connectionPool = LdapUserSearchSessionFactory.createConnectionPool(config, new SingleServerSet("localhost",
                randomFrom(ldapServers).getListenPort()), TimeValue.timeValueSeconds(5), NoOpLogger.INSTANCE);
        try {
            assertThat(connectionPool.getCurrentAvailableConnections(),
                    is(LdapUserSearchSessionFactory.DEFAULT_CONNECTION_POOL_INITIAL_SIZE));
            assertThat(connectionPool.getMaximumAvailableConnections(),
                    is(LdapUserSearchSessionFactory.DEFAULT_CONNECTION_POOL_SIZE));
            assertEquals(connectionPool.getHealthCheck().getClass(), GetEntryLDAPConnectionPoolHealthCheck.class);
            GetEntryLDAPConnectionPoolHealthCheck healthCheck = (GetEntryLDAPConnectionPoolHealthCheck) connectionPool.getHealthCheck();
            assertThat(healthCheck.getEntryDN(), is("cn=Horatio Hornblower,ou=people,o=sevenSeas"));
            assertThat(healthCheck.getMaxResponseTimeMillis(), is(LdapUserSearchSessionFactory.TIMEOUT_DEFAULT.millis()));
        } finally {
            connectionPool.close();
        }
    }

    public void testConnectionPoolSettings() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userSearchBase = "o=sevenSeas";
        RealmConfig config = new RealmConfig("ldap_realm", Settings.builder()
                .put(buildLdapSettings(ldapUrls(), Strings.EMPTY_ARRAY, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put("user_search.base_dn", userSearchBase)
                .put("bind_dn", "cn=Horatio Hornblower,ou=people,o=sevenSeas")
                .put("bind_password", "pass")
                .put("user_search.pool.initial_size", 10)
                .put("user_search.pool.size", 12)
                .put("user_search.pool.health_check.enabled", false)
                .build(), globalSettings);

        LDAPConnectionPool connectionPool = LdapUserSearchSessionFactory.createConnectionPool(config, new SingleServerSet("localhost",
                randomFrom(ldapServers).getListenPort()), TimeValue.timeValueSeconds(5), NoOpLogger.INSTANCE);
        try {
            assertThat(connectionPool.getCurrentAvailableConnections(), is(10));
            assertThat(connectionPool.getMaximumAvailableConnections(), is(12));
            assertThat(connectionPool.retryFailedOperationsDueToInvalidConnections(), is(true));
            assertEquals(connectionPool.getHealthCheck().getClass(), LDAPConnectionPoolHealthCheck.class);
        } finally {
            connectionPool.close();
        }
    }

    public void testThatEmptyBindDNWithHealthCheckEnabledDoesNotThrow() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userSearchBase = "o=sevenSeas";
        RealmConfig config = new RealmConfig("ldap_realm", Settings.builder()
                .put(buildLdapSettings(ldapUrls(), Strings.EMPTY_ARRAY, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put("user_search.base_dn", userSearchBase)
                .put("bind_password", "pass")
                .build(), globalSettings);

        LdapUserSearchSessionFactory searchSessionFactory = null;
        try {
            searchSessionFactory = new LdapUserSearchSessionFactory(config, sslService);
        } finally {
            if (searchSessionFactory != null) {
                searchSessionFactory.shutdown();
            }
        }
    }

    public void testEmptyBindDNReturnsNullBindRequest() {
        BindRequest request = LdapUserSearchSessionFactory.bindRequest(Settings.builder().put("bind_password", "password").build());
        assertThat(request, is(nullValue()));
    }

    public void testThatBindRequestReturnsSimpleBindRequest() {
        BindRequest request = LdapUserSearchSessionFactory.bindRequest(Settings.builder()
                .put("bind_password", "password")
                .put("bind_dn", "cn=ironman")
                .build());
        assertEquals(request.getClass(), SimpleBindRequest.class);
        SimpleBindRequest simpleBindRequest = (SimpleBindRequest) request;
        assertThat(simpleBindRequest.getBindDN(), is("cn=ironman"));
    }

    public void testThatConnectErrorIsNotThrownOnConstruction() throws Exception {
        String groupSearchBase = "DC=ad,DC=test,DC=elasticsearch,DC=com";
        String userSearchBase = "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";

        // pick a random ldap server and stop it
        InMemoryDirectoryServer inMemoryDirectoryServer = randomFrom(ldapServers);
        String ldapUrl = new LDAPURL("ldap", "localhost", inMemoryDirectoryServer.getListenPort(), null, null, null, null).toString();
        inMemoryDirectoryServer.shutDown(true);

        Settings ldapSettings = Settings.builder()
                .put(LdapTestCase.buildLdapSettings(new String[] { ldapUrl }, Strings.EMPTY_ARRAY,
                        groupSearchBase, LdapSearchScope.SUB_TREE))
                .put("user_search.base_dn", userSearchBase)
                .put("bind_dn", "ironman@ad.test.elasticsearch.com")
                .put("bind_password", ActiveDirectorySessionFactoryTests.PASSWORD)
                .put("user_search.attribute", "cn")
                .put("timeout.tcp_connect", "500ms")
                .put("type", "ldap")
                .put("user_search.pool.health_check.enabled", false)
                .put("user_search.pool.enabled", randomBoolean())
                .build();

        RealmConfig config = new RealmConfig("ldap_realm", ldapSettings, globalSettings);
        LdapUserSearchSessionFactory searchSessionFactory = null;
        try {
            searchSessionFactory = new LdapUserSearchSessionFactory(config, sslService);
        } finally {
            if (searchSessionFactory != null) {
                searchSessionFactory.shutdown();
            }
        }
    }
}
