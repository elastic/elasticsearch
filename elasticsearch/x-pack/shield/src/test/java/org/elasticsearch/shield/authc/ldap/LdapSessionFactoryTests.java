/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.sdk.LDAPURL;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.shield.authc.ldap.support.LdapSession;
import org.elasticsearch.shield.authc.ldap.support.LdapTestCase;
import org.elasticsearch.shield.authc.ldap.support.SessionFactory;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.SecuredStringTests;
import org.elasticsearch.test.junit.annotations.Network;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

public class LdapSessionFactoryTests extends LdapTestCase {
    private Settings globalSettings;

    @Before
    public void setup() {
        globalSettings = Settings.builder().put("path.home", createTempDir()).build();
    }

    public void testBindWithReadTimeout() throws Exception {
        InMemoryDirectoryServer ldapServer = randomFrom(ldapServers);
        String ldapUrl = new LDAPURL("ldap", "localhost", ldapServer.getListenPort(), null, null, null, null).toString();
        String groupSearchBase = "o=sevenSeas";
        String userTemplates = "cn={0},ou=people,o=sevenSeas";

        Settings settings = Settings.builder()
                .put(buildLdapSettings(ldapUrl, userTemplates, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put(SessionFactory.TIMEOUT_TCP_READ_SETTING, "1ms") //1 millisecond
                .put("path.home", createTempDir())
                .build();

        RealmConfig config = new RealmConfig("ldap_realm", settings, globalSettings);
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, null).init();
        String user = "Horatio Hornblower";
        SecuredString userPass = SecuredStringTests.build("pass");

        ldapServer.setProcessingDelayMillis(500L);
        try (LdapSession session = sessionFactory.session(user, userPass)) {
            fail("expected connection timeout error here");
        } catch (Throwable t) {
            assertThat(t, instanceOf(ElasticsearchSecurityException.class));
            assertThat(t.getCause().getMessage(), containsString("A client-side timeout was encountered while waiting "));
        } finally {
            ldapServer.setProcessingDelayMillis(0L);
        }
    }

    @Network
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch-shield/issues/767")
    public void testConnectTimeout() {
        // Local sockets connect too fast...
        String ldapUrl = "ldap://54.200.235.244:389";
        String groupSearchBase = "o=sevenSeas";
        String userTemplates = "cn={0},ou=people,o=sevenSeas";

        Settings settings = Settings.builder()
                .put(buildLdapSettings(ldapUrl, userTemplates, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put(SessionFactory.TIMEOUT_TCP_CONNECTION_SETTING, "1ms") //1 millisecond
                .build();

        RealmConfig config = new RealmConfig("ldap_realm", settings, globalSettings);
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, null).init();
        String user = "Horatio Hornblower";
        SecuredString userPass = SecuredStringTests.build("pass");

        long start = System.currentTimeMillis();
        try (LdapSession session = sessionFactory.session(user, userPass)) {
            fail("expected connection timeout error here");
        } catch (Throwable t) {
            long time = System.currentTimeMillis() - start;
            assertThat(time, lessThan(10000L));
            assertThat(t, instanceOf(IOException.class));
            assertThat(t.getCause().getCause().getMessage(), containsString("within the configured timeout of"));
        }
    }

    public void testBindWithTemplates() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String[] userTemplates = new String[] {
                "cn={0},ou=something,ou=obviously,ou=incorrect,o=sevenSeas",
                "wrongname={0},ou=people,o=sevenSeas",
                "cn={0},ou=people,o=sevenSeas", //this last one should work
        };
        RealmConfig config = new RealmConfig("ldap_realm", buildLdapSettings(ldapUrls(), userTemplates, groupSearchBase,
                LdapSearchScope.SUB_TREE), globalSettings);

        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, null).init();

        String user = "Horatio Hornblower";
        SecuredString userPass = SecuredStringTests.build("pass");

        try (LdapSession ldap = sessionFactory.session(user, userPass)) {
            String dn = ldap.userDn();
            assertThat(dn, containsString(user));
        }
    }

    public void testBindWithBogusTemplates() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String[] userTemplates = new String[] {
                "cn={0},ou=something,ou=obviously,ou=incorrect,o=sevenSeas",
                "wrongname={0},ou=people,o=sevenSeas",
                "asdf={0},ou=people,o=sevenSeas", //none of these should work
        };
        RealmConfig config = new RealmConfig("ldap_realm", buildLdapSettings(ldapUrls(), userTemplates, groupSearchBase,
                LdapSearchScope.SUB_TREE), globalSettings);

        LdapSessionFactory ldapFac = new LdapSessionFactory(config, null).init();

        String user = "Horatio Hornblower";
        SecuredString userPass = SecuredStringTests.build("pass");
        try (LdapSession ldapConnection = ldapFac.session(user, userPass)) {
            fail("Expected ElasticsearchSecurityException");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.getMessage(), is("failed LDAP authentication"));
        }
    }

    public void testGroupLookupSubtree() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = "cn={0},ou=people,o=sevenSeas";
        RealmConfig config = new RealmConfig("ldap_realm", buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase,
                LdapSearchScope.SUB_TREE), globalSettings);

        LdapSessionFactory ldapFac = new LdapSessionFactory(config, null).init();

        String user = "Horatio Hornblower";
        SecuredString userPass = SecuredStringTests.build("pass");

        try (LdapSession ldap = ldapFac.session(user, userPass)) {
            List<String> groups = ldap.groups();
            assertThat(groups, contains("cn=HMS Lydia,ou=crews,ou=groups,o=sevenSeas"));
        }
    }

    public void testGroupLookupOneLevel() throws Exception {
        String groupSearchBase = "ou=crews,ou=groups,o=sevenSeas";
        String userTemplate = "cn={0},ou=people,o=sevenSeas";
        RealmConfig config = new RealmConfig("ldap_realm", buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase,
                LdapSearchScope.ONE_LEVEL), globalSettings);

        LdapSessionFactory ldapFac = new LdapSessionFactory(config, null).init();

        String user = "Horatio Hornblower";
        try (LdapSession ldap = ldapFac.session(user, SecuredStringTests.build("pass"))) {
            List<String> groups = ldap.groups();
            assertThat(groups, contains("cn=HMS Lydia,ou=crews,ou=groups,o=sevenSeas"));
        }
    }

    public void testGroupLookupBase() throws Exception {
        String groupSearchBase = "cn=HMS Lydia,ou=crews,ou=groups,o=sevenSeas";
        String userTemplate = "cn={0},ou=people,o=sevenSeas";
        RealmConfig config = new RealmConfig("ldap_realm", buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase,
                LdapSearchScope.BASE), globalSettings);

        LdapSessionFactory ldapFac = new LdapSessionFactory(config, null).init();

        String user = "Horatio Hornblower";
        SecuredString userPass = SecuredStringTests.build("pass");

        try (LdapSession ldap = ldapFac.session(user, userPass)) {
            List<String> groups = ldap.groups();
            assertThat(groups.size(), is(1));
            assertThat(groups, contains("cn=HMS Lydia,ou=crews,ou=groups,o=sevenSeas"));
        }
    }
}
