/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.shield.authc.ldap.support.LdapSession;
import org.elasticsearch.shield.authc.ldap.support.LdapTest;
import org.elasticsearch.shield.authc.ldap.support.SessionFactory;
import org.elasticsearch.shield.authc.support.SecuredStringTests;
import org.elasticsearch.shield.ssl.ClientSSLService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.junit.annotations.Network;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;

@Network
public class OpenLdapTests extends ElasticsearchTestCase {

    public static final String OPEN_LDAP_URL = "ldaps://54.200.235.244:636";
    public static final String PASSWORD = "NickFuryHeartsES";

    public ClientSSLService clientSSLService;

    @Before
    public void initializeSslSocketFactory() throws Exception {
        Path keystore = Paths.get(OpenLdapTests.class.getResource("../ldap/support/ldaptrust.jks").toURI()).toAbsolutePath();

        /*
         * Prior to each test we reinitialize the socket factory with a new SSLService so that we get a new SSLContext.
         * If we re-use a SSLContext, previously connected sessions can get re-established which breaks hostname
         * verification tests since a re-established connection does not perform hostname verification.
         */
        clientSSLService = new ClientSSLService(ImmutableSettings.builder()
                .put("shield.ssl.keystore.path", keystore)
                .put("shield.ssl.keystore.password", "changeit")
                .build());
    }

    @Test
    public void testConnect() {
        //openldap does not use cn as naming attributes by default
        String groupSearchBase = "ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        String userTemplate = "uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        RealmConfig config = new RealmConfig("oldap-test", LdapTest.buildLdapSettings(OPEN_LDAP_URL, userTemplate, groupSearchBase, LdapSearchScope.ONE_LEVEL));
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, clientSSLService);

        String[] users = new String[] { "blackwidow", "cap", "hawkeye", "hulk", "ironman", "thor" };
        for (String user : users) {
            try (LdapSession ldap = sessionFactory.open(user, SecuredStringTests.build(PASSWORD))) {
                assertThat(ldap.groups(), hasItem(containsString("Avengers")));
            }
        }
    }

    @Test
    public void testGroupSearchScopeBase() {
        //base search on a groups means that the user can be in just one group

        String groupSearchBase = "cn=Avengers,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        String userTemplate = "uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        RealmConfig config = new RealmConfig("oldap-test", LdapTest.buildLdapSettings(OPEN_LDAP_URL, userTemplate, groupSearchBase, LdapSearchScope.BASE));
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, clientSSLService);

        String[] users = new String[] { "blackwidow", "cap", "hawkeye", "hulk", "ironman", "thor" };
        for (String user : users) {
            LdapSession ldap = sessionFactory.open(user, SecuredStringTests.build(PASSWORD));
            assertThat(ldap.groups(), hasItem(containsString("Avengers")));
            ldap.close();
        }
    }

    @Test
    public void testCustomFilter() {
        String groupSearchBase = "ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        String userTemplate = "uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        Settings settings = ImmutableSettings.builder()
                .put(LdapTest.buildLdapSettings(OPEN_LDAP_URL, userTemplate, groupSearchBase, LdapSearchScope.ONE_LEVEL))
                .put("group_search.filter", "(&(objectclass=posixGroup)(memberUID={0}))")
                .put("group_search.user_attribute", "uid")
                .build();
        RealmConfig config = new RealmConfig("oldap-test", settings);
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, clientSSLService);

        try (LdapSession ldap = sessionFactory.open("selvig", SecuredStringTests.build(PASSWORD))){
            assertThat(ldap.groups(), hasItem(containsString("Geniuses")));
        }
    }

    @Test
    public void testTcpTimeout() {
        String groupSearchBase = "ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        String userTemplate = "uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        Settings settings = ImmutableSettings.builder()
                .put(LdapTest.buildLdapSettings(OPEN_LDAP_URL, userTemplate, groupSearchBase, LdapSearchScope.ONE_LEVEL))
                .put(SessionFactory.HOSTNAME_VERIFICATION_SETTING, false)
                .put(SessionFactory.TIMEOUT_TCP_READ_SETTING, "1ms") //1 millisecond
                .build();
        RealmConfig config = new RealmConfig("oldap-test", settings);
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, clientSSLService);

        try (LdapSession ldap = sessionFactory.open("thor", SecuredStringTests.build(PASSWORD))){
            fail("The TCP connection should timeout before getting groups back");
        } catch (ShieldLdapException e) {
            assertThat(e.getCause().getMessage(), containsString("A client-side timeout was encountered while waiting"));
        }
    }

    @Test
    public void testStandardLdapConnectionHostnameVerification() {
        //openldap does not use cn as naming attributes by default
        String groupSearchBase = "ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        String userTemplate = "uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        Settings settings = ImmutableSettings.builder()
                .put(LdapTest.buildLdapSettings(OPEN_LDAP_URL, userTemplate, groupSearchBase, LdapSearchScope.ONE_LEVEL))
                .put(LdapSessionFactory.HOSTNAME_VERIFICATION_SETTING, true)
                .build();

        RealmConfig config = new RealmConfig("oldap-test", settings);
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, clientSSLService);

        String user = "blackwidow";
        try (LdapSession ldap = sessionFactory.open(user, SecuredStringTests.build(PASSWORD))) {
            fail("OpenLDAP certificate does not contain the correct hostname/ip so hostname verification should fail on open");
        } catch (ShieldLdapException e) {
            assertThat(e.getMessage(), containsString("failed to connect to any LDAP servers"));
        }
    }
}
