/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.ldap.support.SessionFactory;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.SecuredStringTests;
import org.elasticsearch.shield.authc.ldap.support.LdapSession;
import org.elasticsearch.shield.authc.ldap.support.LdapTest;
import org.elasticsearch.shield.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.test.junit.annotations.Network;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.*;

public class LdapSessionFactoryTests extends LdapTest {

    @Test
    public void testBindWithReadTimeout() throws Exception {
        String ldapUrl = ldapUrl();
        String groupSearchBase = "o=sevenSeas";
        String[] userTemplates = new String[] {
                "cn={0},ou=people,o=sevenSeas",
        };
        Settings settings = ImmutableSettings.builder()
                .put(buildLdapSettings(ldapUrl, userTemplates, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put(SessionFactory.TIMEOUT_TCP_READ_SETTING, "1ms") //1 millisecond
                .build();

        RealmConfig config = new RealmConfig("ldap_realm", settings);
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, null);
        String user = "Horatio Hornblower";
        SecuredString userPass = SecuredStringTests.build("pass");

        ldapServer.setProcessingDelayMillis(500L);
        long start = System.currentTimeMillis();
        try (LdapSession session = sessionFactory.session(user, userPass)) {
            fail("expected connection timeout error here");
        } catch (Throwable t) {
            long time = System.currentTimeMillis() - start;
            assertThat(time, lessThan(1000l));
            assertThat(t, instanceOf(ShieldLdapException.class));
        } finally {
            ldapServer.setProcessingDelayMillis(0L);
        }
    }

    @Test
    @Network
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-shield/issues/767")
    public void testConnectTimeout() {
        // Local sockets connect too fast...
        String ldapUrl = "ldap://54.200.235.244:389";
        String groupSearchBase = "o=sevenSeas";
        String[] userTemplates = new String[] {
                "cn={0},ou=people,o=sevenSeas",
        };
        Settings settings = ImmutableSettings.builder()
                .put(buildLdapSettings(ldapUrl, userTemplates, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put(SessionFactory.TIMEOUT_TCP_CONNECTION_SETTING, "1ms") //1 millisecond
                .build();

        RealmConfig config = new RealmConfig("ldap_realm", settings);
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, null);
        String user = "Horatio Hornblower";
        SecuredString userPass = SecuredStringTests.build("pass");

        long start = System.currentTimeMillis();
        try (LdapSession session = sessionFactory.session(user, userPass)) {
            fail("expected connection timeout error here");
        } catch (Throwable t) {
            long time = System.currentTimeMillis() - start;
            assertThat(time, lessThan(10000l));
            assertThat(t, instanceOf(ShieldLdapException.class));
            assertThat(t.getCause().getCause().getMessage(), containsString("within the configured timeout of"));
        }
    }

    @Test
    public void testBindWithTemplates() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String[] userTemplates = new String[] {
                "cn={0},ou=something,ou=obviously,ou=incorrect,o=sevenSeas",
                "wrongname={0},ou=people,o=sevenSeas",
                "cn={0},ou=people,o=sevenSeas", //this last one should work
        };
        RealmConfig config = new RealmConfig("ldap_realm", buildLdapSettings(ldapUrl(), userTemplates, groupSearchBase, LdapSearchScope.SUB_TREE));

        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, null);

        String user = "Horatio Hornblower";
        SecuredString userPass = SecuredStringTests.build("pass");

        try (LdapSession ldap = sessionFactory.session(user, userPass)) {
            String dn = ldap.userDn();
            assertThat(dn, containsString(user));
        }
    }


    @Test(expected = ShieldLdapException.class)
    public void testBindWithBogusTemplates() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String[] userTemplates = new String[] {
                "cn={0},ou=something,ou=obviously,ou=incorrect,o=sevenSeas",
                "wrongname={0},ou=people,o=sevenSeas",
                "asdf={0},ou=people,o=sevenSeas", //none of these should work
        };
        RealmConfig config = new RealmConfig("ldap_realm", buildLdapSettings(ldapUrl(), userTemplates, groupSearchBase, LdapSearchScope.SUB_TREE));

        LdapSessionFactory ldapFac = new LdapSessionFactory(config, null);

        String user = "Horatio Hornblower";
        SecuredString userPass = SecuredStringTests.build("pass");
        try (LdapSession ldapConnection = ldapFac.session(user, userPass)) {
        }
    }

    @Test
    public void testGroupLookup_Subtree() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = "cn={0},ou=people,o=sevenSeas";
        RealmConfig config = new RealmConfig("ldap_realm", buildLdapSettings(ldapUrl(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE));

        LdapSessionFactory ldapFac = new LdapSessionFactory(config, null);

        String user = "Horatio Hornblower";
        SecuredString userPass = SecuredStringTests.build("pass");

        try (LdapSession ldap = ldapFac.session(user, userPass)) {
            List<String> groups = ldap.groups();
            assertThat(groups, contains("cn=HMS Lydia,ou=crews,ou=groups,o=sevenSeas"));
        }
    }

    @Test
    public void testGroupLookup_OneLevel() throws Exception {
        String groupSearchBase = "ou=crews,ou=groups,o=sevenSeas";
        String userTemplate = "cn={0},ou=people,o=sevenSeas";
        RealmConfig config = new RealmConfig("ldap_realm", buildLdapSettings(ldapUrl(), userTemplate, groupSearchBase, LdapSearchScope.ONE_LEVEL));

        LdapSessionFactory ldapFac = new LdapSessionFactory(config, null);

        String user = "Horatio Hornblower";
        try (LdapSession ldap = ldapFac.session(user, SecuredStringTests.build("pass"))) {
            List<String> groups = ldap.groups();
            assertThat(groups, contains("cn=HMS Lydia,ou=crews,ou=groups,o=sevenSeas"));
        }
    }

    @Test
    public void testGroupLookup_Base() throws Exception {
        String groupSearchBase = "cn=HMS Lydia,ou=crews,ou=groups,o=sevenSeas";
        String userTemplate = "cn={0},ou=people,o=sevenSeas";
        RealmConfig config = new RealmConfig("ldap_realm", buildLdapSettings(ldapUrl(), userTemplate, groupSearchBase, LdapSearchScope.BASE));

        LdapSessionFactory ldapFac = new LdapSessionFactory(config, null);

        String user = "Horatio Hornblower";
        SecuredString userPass = SecuredStringTests.build("pass");

        try (LdapSession ldap = ldapFac.session(user, userPass)) {
            List<String> groups = ldap.groups();
            assertThat(groups.size(), is(1));
            assertThat(groups, contains("cn=HMS Lydia,ou=crews,ou=groups,o=sevenSeas"));
        }
    }
}
