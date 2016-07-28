/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.LDAPException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapTestCase;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.xpack.security.authc.support.SecuredStringTests;
import org.elasticsearch.xpack.security.ssl.ClientSSLService;
import org.elasticsearch.xpack.security.ssl.SSLConfiguration.Global;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.Network;
import org.junit.Before;

import java.nio.file.Path;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;

@Network
public class OpenLdapTests extends ESTestCase {

    public static final String OPEN_LDAP_URL = "ldaps://54.200.235.244:636";
    public static final String PASSWORD = "NickFuryHeartsES";

    private boolean useGlobalSSL;
    private ClientSSLService clientSSLService;
    private Settings globalSettings;

    @Before
    public void initializeSslSocketFactory() throws Exception {
        Path keystore = getDataPath("../ldap/support/ldaptrust.jks");
        /*
         * Prior to each test we reinitialize the socket factory with a new SSLService so that we get a new SSLContext.
         * If we re-use a SSLContext, previously connected sessions can get re-established which breaks hostname
         * verification tests since a re-established connection does not perform hostname verification.
         */
        useGlobalSSL = randomBoolean();
        Settings.Builder builder = Settings.builder().put("path.home", createTempDir());
        if (useGlobalSSL) {
            builder.put("xpack.security.ssl.keystore.path", keystore)
                    .put("xpack.security.ssl.keystore.password", "changeit");
        }
        globalSettings = builder.build();
        Environment environment = new Environment(globalSettings);
        clientSSLService = new ClientSSLService(globalSettings, environment, new Global(globalSettings));
    }

    public void testConnect() throws Exception {
        //openldap does not use cn as naming attributes by default
        String groupSearchBase = "ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        String userTemplate = "uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        RealmConfig config = new RealmConfig("oldap-test", buildLdapSettings(OPEN_LDAP_URL, userTemplate, groupSearchBase,
                LdapSearchScope.ONE_LEVEL), globalSettings);
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, clientSSLService);

        String[] users = new String[] { "blackwidow", "cap", "hawkeye", "hulk", "ironman", "thor" };
        for (String user : users) {
            try (LdapSession ldap = sessionFactory.session(user, SecuredStringTests.build(PASSWORD))) {
                assertThat(ldap.groups(), hasItem(containsString("Avengers")));
            }
        }
    }

    public void testGroupSearchScopeBase() throws Exception {
        //base search on a groups means that the user can be in just one group

        String groupSearchBase = "cn=Avengers,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        String userTemplate = "uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        RealmConfig config = new RealmConfig("oldap-test", buildLdapSettings(OPEN_LDAP_URL, userTemplate, groupSearchBase,
                LdapSearchScope.BASE), globalSettings);
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, clientSSLService);

        String[] users = new String[] { "blackwidow", "cap", "hawkeye", "hulk", "ironman", "thor" };
        for (String user : users) {
            LdapSession ldap = sessionFactory.session(user, SecuredStringTests.build(PASSWORD));
            assertThat(ldap.groups(), hasItem(containsString("Avengers")));
            ldap.close();
        }
    }

    public void testCustomFilter() throws Exception {
        String groupSearchBase = "ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        String userTemplate = "uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        Settings settings = Settings.builder()
                .put(buildLdapSettings(OPEN_LDAP_URL, userTemplate, groupSearchBase, LdapSearchScope.ONE_LEVEL))
                .put("group_search.filter", "(&(objectclass=posixGroup)(memberUID={0}))")
                .put("group_search.user_attribute", "uid")
                .build();
        RealmConfig config = new RealmConfig("oldap-test", settings, globalSettings);
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, clientSSLService);

        try (LdapSession ldap = sessionFactory.session("selvig", SecuredStringTests.build(PASSWORD))){
            assertThat(ldap.groups(), hasItem(containsString("Geniuses")));
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/x-plugins/issues/2849")
    public void testTcpTimeout() throws Exception {
        String groupSearchBase = "ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        String userTemplate = "uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        Settings settings = Settings.builder()
                .put(buildLdapSettings(OPEN_LDAP_URL, userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put("group_search.filter", "(objectClass=*)")
                .put(SessionFactory.HOSTNAME_VERIFICATION_SETTING, false)
                .put(SessionFactory.TIMEOUT_TCP_READ_SETTING, "1ms") //1 millisecond
                .build();
        RealmConfig config = new RealmConfig("oldap-test", settings, globalSettings);
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, clientSSLService);

        LDAPException expected = expectThrows(LDAPException.class,
                () -> sessionFactory.session("thor", SecuredStringTests.build(PASSWORD)).groups());
        assertThat(expected.getMessage(), containsString("A client-side timeout was encountered while waiting"));
    }

    public void testStandardLdapConnectionHostnameVerification() throws Exception {
        //openldap does not use cn as naming attributes by default
        String groupSearchBase = "ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        String userTemplate = "uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        Settings settings = Settings.builder()
                .put(buildLdapSettings(OPEN_LDAP_URL, userTemplate, groupSearchBase, LdapSearchScope.ONE_LEVEL))
                .put(LdapSessionFactory.HOSTNAME_VERIFICATION_SETTING, true)
                .build();

        RealmConfig config = new RealmConfig("oldap-test", settings, globalSettings);
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, clientSSLService);

        String user = "blackwidow";
        LDAPException expected = expectThrows(LDAPException.class, () -> sessionFactory.session(user, SecuredStringTests.build(PASSWORD)));
        assertThat(expected.getMessage(), anyOf(containsString("Hostname verification failed"), containsString("peer not authenticated")));
    }

    Settings buildLdapSettings(String ldapUrl, String userTemplate, String groupSearchBase, LdapSearchScope scope) {
        Settings baseSettings = LdapTestCase.buildLdapSettings(ldapUrl, userTemplate, groupSearchBase, scope);
        if (useGlobalSSL) {
            return baseSettings;
        }
        return Settings.builder()
                .put(baseSettings)
                .put("ssl.truststore.path", getDataPath("../ldap/support/ldaptrust.jks"))
                .put("ssl.truststore.password", "changeit")
                .build();
    }
}
