/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.unboundid.ldap.sdk.BindRequest;
import com.unboundid.ldap.sdk.GetEntryLDAPConnectionPoolHealthCheck;
import com.unboundid.ldap.sdk.LDAPConnectionPool;
import com.unboundid.ldap.sdk.LDAPConnectionPoolHealthCheck;
import com.unboundid.ldap.sdk.SimpleBindRequest;
import com.unboundid.ldap.sdk.SingleServerSet;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.activedirectory.ActiveDirectorySessionFactoryTests;
import org.elasticsearch.shield.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.shield.authc.ldap.support.LdapSession;
import org.elasticsearch.shield.authc.ldap.support.LdapTestCase;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.SecuredStringTests;
import org.elasticsearch.shield.ssl.ClientSSLService;
import org.elasticsearch.shield.ssl.SSLConfiguration.Global;
import org.elasticsearch.shield.support.NoOpLogger;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.watcher.Watcher;
import org.elasticsearch.xpack.XPackPlugin;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.ShieldTestsUtils.assertAuthenticationException;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

// thread leak filter for UnboundID's background connect threads. The background connect threads do not always respect the
// timeout and linger. Will be fixed in a new version of the library, see
// http://sourceforge.net/p/ldap-sdk/discussion/1001257/thread/154e3b71/
@ThreadLeakFilters(filters = {
        LdapUserSearchSessionFactoryTests.BackgroundConnectThreadLeakFilter.class
})
public class LdapUserSearchSessionFactoryTests extends LdapTestCase {
    private ClientSSLService clientSSLService;
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
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", keystore)
                .put("xpack.security.ssl.keystore.password", "changeit")
                .build();
        clientSSLService = new ClientSSLService(settings, new Global(settings));
        clientSSLService.setEnvironment(env);

        globalSettings = Settings.builder().put("path.home", createTempDir()).build();
    }

    public void testSupportsUnauthenticatedSessions() throws Exception {
        RealmConfig config = new RealmConfig("ldap_realm", Settings.builder()
                .put(buildLdapSettings(ldapUrls(), Strings.EMPTY_ARRAY, "", LdapSearchScope.SUB_TREE))
                .put("user_search.base_dn", "")
                .put("bind_dn", "cn=Horatio Hornblower,ou=people,o=sevenSeas")
                .put("bind_password", "pass")
                .put("user_search.attribute", "cn")
                .build(), globalSettings);

        LdapUserSearchSessionFactory sessionFactory = new LdapUserSearchSessionFactory(config, null).init();
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
                .build(), globalSettings);

        LdapUserSearchSessionFactory sessionFactory = new LdapUserSearchSessionFactory(config, null).init();

        String user = "William Bush";
        SecuredString userPass = SecuredStringTests.build("pass");

        try {
            // auth
            try (LdapSession ldap = sessionFactory.session(user, userPass)) {
                String dn = ldap.userDn();
                assertThat(dn, containsString(user));
            }

            //lookup
            try (LdapSession ldap = sessionFactory.unauthenticatedSession(user)) {
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
                .build(), globalSettings);

        LdapUserSearchSessionFactory sessionFactory = new LdapUserSearchSessionFactory(config, null).init();

        String user = "William Bush";
        SecuredString userPass = SecuredStringTests.build("pass");

        try {
            //auth
            try (LdapSession ldap = sessionFactory.session(user, userPass)) {
                fail("the user should not have been found");
            } catch (ElasticsearchSecurityException e) {
                assertAuthenticationException(e, containsString("failed to find user [William Bush] with search base [o=sevenSeas] scope " +
                        "[base]"));
            }

            //lookup
            try (LdapSession ldap = sessionFactory.unauthenticatedSession(user)) {
                fail("the user should not have been found");
            } catch (ElasticsearchSecurityException e) {
                assertAuthenticationException(e, containsString("failed to find user [William Bush] with search base [o=sevenSeas] scope " +
                        "[base]"));
            }
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
                .build(), globalSettings);

        LdapUserSearchSessionFactory sessionFactory = new LdapUserSearchSessionFactory(config, null).init();

        String user = "William Bush";
        SecuredString userPass = SecuredStringTests.build("pass");

        try {
            // auth
            try (LdapSession ldap = sessionFactory.session(user, userPass)) {
                String dn = ldap.userDn();
                assertThat(dn, containsString(user));
            }

            //lookup
            try (LdapSession ldap = sessionFactory.unauthenticatedSession(user)) {
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
                .build(), globalSettings);

        LdapUserSearchSessionFactory sessionFactory = new LdapUserSearchSessionFactory(config, null).init();

        String user = "William Bush";
        SecuredString userPass = SecuredStringTests.build("pass");

        try {
            // auth
            try (LdapSession ldap = sessionFactory.session(user, userPass)) {
                fail("the user should not have been found");
            } catch (ElasticsearchSecurityException e) {
                assertAuthenticationException(e, containsString("failed to find user [William Bush] with search base [o=sevenSeas] scope " +
                        "[one_level]"));
            }

            //lookup
            try (LdapSession ldap = sessionFactory.unauthenticatedSession(user)) {
                fail("the user should not have been found");
            } catch (ElasticsearchSecurityException e) {
                assertAuthenticationException(e, containsString("failed to find user [William Bush] with search base [o=sevenSeas] scope " +
                        "[one_level]"));
            }
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
                .build(), globalSettings);

        LdapUserSearchSessionFactory sessionFactory = new LdapUserSearchSessionFactory(config, null).init();

        String user = "William Bush";
        SecuredString userPass = SecuredStringTests.build("pass");

        try {
            //auth
            try (LdapSession ldap = sessionFactory.session(user, userPass)) {
                String dn = ldap.userDn();
                assertThat(dn, containsString(user));
            }

            //lookup
            try (LdapSession ldap = sessionFactory.unauthenticatedSession(user)) {
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
                .build(), globalSettings);

        LdapUserSearchSessionFactory sessionFactory = new LdapUserSearchSessionFactory(config, null).init();

        String user = "William Bush";
        SecuredString userPass = SecuredStringTests.build("pass");

        try {
            //auth
            try (LdapSession ldap = sessionFactory.session(user, userPass)) {
                fail("the user should not have been found");
            } catch (ElasticsearchSecurityException e) {
                assertAuthenticationException(e, containsString("failed to find user [William Bush] with search base [o=sevenSeas] scope " +
                        "[sub_tree]"));
            }

            //lookup
            try (LdapSession ldap = sessionFactory.unauthenticatedSession(user)) {
                fail("the user should not have been found");
            } catch (ElasticsearchSecurityException e) {
                assertAuthenticationException(e, containsString("failed to find user [William Bush] with search base [o=sevenSeas] scope " +
                        "[sub_tree]"));
            }
        }finally {
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
                .build(), globalSettings);

        LdapUserSearchSessionFactory sessionFactory = new LdapUserSearchSessionFactory(config, null).init();

        String user = "wbush";
        SecuredString userPass = SecuredStringTests.build("pass");

        try {
            //auth
            try (LdapSession ldap = sessionFactory.session(user, userPass)) {
                String dn = ldap.userDn();
                assertThat(dn, containsString("William Bush"));
            }

            //lookup
            try (LdapSession ldap = sessionFactory.unauthenticatedSession(user)) {
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
                .build();
        RealmConfig config = new RealmConfig("ad-as-ldap-test", settings, globalSettings);
        LdapUserSearchSessionFactory sessionFactory = new LdapUserSearchSessionFactory(config, clientSSLService).init();

        String user = "Bruce Banner";
        try {
            //auth
            try (LdapSession ldap = sessionFactory.session(user, SecuredStringTests.build(ActiveDirectorySessionFactoryTests.PASSWORD))) {
                List<String> groups = ldap.groups();

                assertThat(groups, containsInAnyOrder(
                        containsString("Avengers"),
                        containsString("SHIELD"),
                        containsString("Geniuses"),
                        containsString("Philanthropists")));
            }

            //lookup
            try (LdapSession ldap = sessionFactory.unauthenticatedSession(user)) {
                List<String> groups = ldap.groups();

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
                .build(), globalSettings);
        LdapUserSearchSessionFactory sessionFactory = new LdapUserSearchSessionFactory(config, clientSSLService).init();

        String[] users = new String[] { "cap", "hawkeye", "hulk", "ironman", "thor" };
        try {
            for (String user : users) {
                //auth
                try (LdapSession ldap = sessionFactory.session(user, SecuredStringTests.build(OpenLdapTests.PASSWORD))) {
                    assertThat(ldap.userDn(), is(equalTo(new MessageFormat("uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com",
                            Locale.ROOT).format(new Object[]{user}, new StringBuffer(), null).toString())));
                    assertThat(ldap.groups(), hasItem(containsString("Avengers")));
                }

                //lookup
                try (LdapSession ldap = sessionFactory.unauthenticatedSession(user)) {
                    assertThat(ldap.userDn(), is(equalTo(new MessageFormat("uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com",
                            Locale.ROOT).format(new Object[]{user}, new StringBuffer(), null).toString())));
                    assertThat(ldap.groups(), hasItem(containsString("Avengers")));
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

    public void testThatEmptyBindDNThrowsExceptionWithHealthCheckEnabled() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userSearchBase = "o=sevenSeas";
        RealmConfig config = new RealmConfig("ldap_realm", Settings.builder()
                .put(buildLdapSettings(ldapUrls(), Strings.EMPTY_ARRAY, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put("user_search.base_dn", userSearchBase)
                .put("bind_password", "pass")
                .build(), globalSettings);

        try {
            new LdapUserSearchSessionFactory(config, null).init();
            fail("expected an exception");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("[bind_dn] has not been specified so a value must be specified for [user_search" +
                    ".pool.health_check.dn] or [user_search.pool.health_check.enabled] must be set to false"));
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

    @Network
    public void testThatLDAPServerConnectErrorDoesNotPreventNodeFromStarting() throws IOException {
        String groupSearchBase = "DC=ad,DC=test,DC=elasticsearch,DC=com";
        String userSearchBase = "CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
        Settings ldapSettings = Settings.builder()
                .put(LdapTestCase.buildLdapSettings(new String[] { "ldaps://elastic.co:636" }, Strings.EMPTY_ARRAY,
                        groupSearchBase, LdapSearchScope.SUB_TREE))
                .put("user_search.base_dn", userSearchBase)
                .put("bind_dn", "ironman@ad.test.elasticsearch.com")
                .put("bind_password", ActiveDirectorySessionFactoryTests.PASSWORD)
                .put("user_search.attribute", "cn")
                .put("timeout.tcp_connect", "500ms")
                .put("type", "ldap")
                .build();

        Settings.Builder builder = Settings.builder();
        for (Map.Entry<String, String> entry : ldapSettings.getAsMap().entrySet()) {
            builder.put("xpack.security.authc.realms.ldap1." + entry.getKey(), entry.getValue());
        }
        builder.put("path.home", createTempDir());

        // disable watcher, because watcher takes some time when starting, which results in problems
        // having a quick start/stop cycle like below
        builder.put(XPackPlugin.featureEnabledSetting(Watcher.NAME), false);

        try (Node node = new MockNode(builder.build(), Version.CURRENT, Collections.singletonList(XPackPlugin.class))) {
            node.start();
        }
    }

    public static class BackgroundConnectThreadLeakFilter implements ThreadFilter {
        @Override
        public boolean reject(Thread thread) {
            if (thread.getName().startsWith("Background connect thread for elastic.co")) {
                return true;
            }
            return false;
        }
    }
}
