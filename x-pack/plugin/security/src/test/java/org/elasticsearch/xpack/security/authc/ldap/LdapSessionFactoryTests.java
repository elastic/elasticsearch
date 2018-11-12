/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPURL;
import com.unboundid.ldap.sdk.SimpleBindRequest;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class LdapSessionFactoryTests extends LdapTestCase {
    private Settings globalSettings;
    private SSLService sslService;
    private ThreadPool threadPool;

    @Before
    public void setup() throws Exception {
        globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        sslService = new SSLService(globalSettings, TestEnvironment.newEnvironment(globalSettings));
        threadPool = new TestThreadPool("LdapSessionFactoryTests thread pool");
    }

    @After
    public void shutdown() throws InterruptedException {
        terminate(threadPool);
    }

    public void testBindWithReadTimeout() throws Exception {
        InMemoryDirectoryServer ldapServer = randomFrom(ldapServers);
        String ldapUrl = new LDAPURL("ldap", "localhost", ldapServer.getListenPort(), null, null, null, null).toString();
        String groupSearchBase = "o=sevenSeas";
        String userTemplates = "cn={0},ou=people,o=sevenSeas";

        Settings settings = Settings.builder()
                .put(globalSettings)
                .put(buildLdapSettings(ldapUrl, userTemplates, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put(RealmSettings.getFullSettingKey(REALM_IDENTIFIER, SessionFactorySettings.TIMEOUT_TCP_READ_SETTING), "1ms")
                .put("path.home", createTempDir())
                .build();

        RealmConfig config = new RealmConfig(REALM_IDENTIFIER, settings,
                TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, sslService, threadPool);
        String user = "Horatio Hornblower";
        SecureString userPass = new SecureString("pass");

        ldapServer.setProcessingDelayMillis(500L);
        try {
            UncategorizedExecutionException e =
                    expectThrows(UncategorizedExecutionException.class, () -> session(sessionFactory, user, userPass));
            assertThat(e.getCause(), instanceOf(ExecutionException.class));
            assertThat(e.getCause().getCause(), instanceOf(LDAPException.class));
            assertThat(e.getCause().getCause().getMessage(), containsString("A client-side timeout was encountered while waiting "));
        } finally {
            ldapServer.setProcessingDelayMillis(0L);
        }
    }

    public void testBindWithTemplates() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String[] userTemplates = new String[]{
                "cn={0},ou=something,ou=obviously,ou=incorrect,o=sevenSeas",
                "wrongname={0},ou=people,o=sevenSeas",
                "cn={0},ou=people,o=sevenSeas", //this last one should work
        };
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put(buildLdapSettings(ldapUrls(), userTemplates, groupSearchBase, LdapSearchScope.SUB_TREE))
            .build();
        RealmConfig config = new RealmConfig(REALM_IDENTIFIER, settings,
                TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));

        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, sslService, threadPool);

        String user = "Horatio Hornblower";
        SecureString userPass = new SecureString("pass");
        final SimpleBindRequest bindRequest = new SimpleBindRequest("cn=Horatio Hornblower,ou=people,o=sevenSeas", "pass");

        try (LdapSession ldap = session(sessionFactory, user, userPass)) {
            assertConnectionValid(ldap.getConnection(), bindRequest);
            String dn = ldap.userDn();
            assertThat(dn, containsString(user));
        }
    }

    public void testBindWithBogusTemplates() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String[] userTemplates = new String[]{
                "cn={0},ou=something,ou=obviously,ou=incorrect,o=sevenSeas",
                "wrongname={0},ou=people,o=sevenSeas",
                "asdf={0},ou=people,o=sevenSeas", //none of these should work
        };
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put(buildLdapSettings(ldapUrls(), userTemplates, groupSearchBase, LdapSearchScope.SUB_TREE))
            .build();
        RealmConfig config = new RealmConfig(REALM_IDENTIFIER, settings,
                TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));

        LdapSessionFactory ldapFac = new LdapSessionFactory(config, sslService, threadPool);

        String user = "Horatio Hornblower";
        SecureString userPass = new SecureString("pass");
        UncategorizedExecutionException e = expectThrows(UncategorizedExecutionException.class, () -> session(ldapFac, user, userPass));
        assertThat(e.getCause(), instanceOf(ExecutionException.class));
        assertThat(e.getCause().getCause(), instanceOf(LDAPException.class));
        assertThat(e.getCause().getCause().getMessage(), containsString("Unable to bind as user"));
        Throwable[] suppressed = e.getCause().getCause().getSuppressed();
        assertThat(suppressed.length, is(2));
    }

    public void testGroupLookupSubtree() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = "cn={0},ou=people,o=sevenSeas";
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
            .build();
        RealmConfig config = new RealmConfig(REALM_IDENTIFIER, settings,
            TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));

        LdapSessionFactory ldapFac = new LdapSessionFactory(config, sslService, threadPool);

        String user = "Horatio Hornblower";
        SecureString userPass = new SecureString("pass");
        final SimpleBindRequest bindRequest = new SimpleBindRequest("cn=Horatio Hornblower,ou=people,o=sevenSeas", "pass");

        try (LdapSession ldap = session(ldapFac, user, userPass)) {
            assertConnectionValid(ldap.getConnection(), bindRequest);
            List<String> groups = groups(ldap);
            assertThat(groups, contains("cn=HMS Lydia,ou=crews,ou=groups,o=sevenSeas"));
        }
    }

    public void testGroupLookupOneLevel() throws Exception {
        String groupSearchBase = "ou=crews,ou=groups,o=sevenSeas";
        String userTemplate = "cn={0},ou=people,o=sevenSeas";
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.ONE_LEVEL))
            .build();
        RealmConfig config = new RealmConfig(REALM_IDENTIFIER, settings,
                TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));

        LdapSessionFactory ldapFac = new LdapSessionFactory(config, sslService, threadPool);

        String user = "Horatio Hornblower";
        final SimpleBindRequest bindRequest = new SimpleBindRequest("cn=Horatio Hornblower,ou=people,o=sevenSeas", "pass");

        try (LdapSession ldap = session(ldapFac, user, new SecureString("pass"))) {
            assertConnectionValid(ldap.getConnection(), bindRequest);
            List<String> groups = groups(ldap);
            assertThat(groups, contains("cn=HMS Lydia,ou=crews,ou=groups,o=sevenSeas"));
        }
    }

    public void testGroupLookupBase() throws Exception {
        String groupSearchBase = "cn=HMS Lydia,ou=crews,ou=groups,o=sevenSeas";
        String userTemplate = "cn={0},ou=people,o=sevenSeas";
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.BASE))
            .build();
        RealmConfig config = new RealmConfig(REALM_IDENTIFIER, settings,
                TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));

        LdapSessionFactory ldapFac = new LdapSessionFactory(config, sslService, threadPool);

        String user = "Horatio Hornblower";
        SecureString userPass = new SecureString("pass");
        final SimpleBindRequest bindRequest = new SimpleBindRequest("cn=Horatio Hornblower,ou=people,o=sevenSeas", "pass");

        try (LdapSession ldap = session(ldapFac, user, userPass)) {
            assertConnectionValid(ldap.getConnection(), bindRequest);
            List<String> groups = groups(ldap);
            assertThat(groups.size(), is(1));
            assertThat(groups, contains("cn=HMS Lydia,ou=crews,ou=groups,o=sevenSeas"));
        }
    }
}
