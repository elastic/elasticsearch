/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.LDAPException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSession;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapTestCase;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.elasticsearch.xpack.security.authc.support.SecuredStringTests;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.Network;
import org.elasticsearch.xpack.ssl.SSLService;
import org.elasticsearch.xpack.ssl.VerificationMode;
import org.junit.Before;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;

@Network
public class OpenLdapTests extends ESTestCase {

    public static final String OPEN_LDAP_URL = "ldaps://54.200.235.244:636";
    public static final String PASSWORD = "NickFuryHeartsES";

    private boolean useGlobalSSL;
    private SSLService sslService;
    private Settings globalSettings;

    @Before
    public void initializeSslSocketFactory() throws Exception {
        Path truststore = getDataPath("../ldap/support/ldaptrust.jks");
        /*
         * Prior to each test we reinitialize the socket factory with a new SSLService so that we get a new SSLContext.
         * If we re-use a SSLContext, previously connected sessions can get re-established which breaks hostname
         * verification tests since a re-established connection does not perform hostname verification.
         */
        useGlobalSSL = randomBoolean();
        Settings.Builder builder = Settings.builder().put("path.home", createTempDir());
        if (useGlobalSSL) {
            builder.put("xpack.ssl.truststore.path", truststore)
                    .put("xpack.ssl.truststore.password", "changeit");

            // fake realm to load config with certificate verification mode
            builder.put("xpack.security.authc.realms.bar.ssl.truststore.path", truststore);
            builder.put("xpack.security.authc.realms.bar.ssl.truststore.password", "changeit");
            builder.put("xpack.security.authc.realms.bar.ssl.verification_mode", VerificationMode.CERTIFICATE);
        } else {
            // fake realms so ssl will get loaded
            builder.put("xpack.security.authc.realms.foo.ssl.truststore.path", truststore);
            builder.put("xpack.security.authc.realms.foo.ssl.truststore.password", "changeit");
            builder.put("xpack.security.authc.realms.foo.ssl.verification_mode", VerificationMode.FULL);
            builder.put("xpack.security.authc.realms.bar.ssl.truststore.path", truststore);
            builder.put("xpack.security.authc.realms.bar.ssl.truststore.password", "changeit");
            builder.put("xpack.security.authc.realms.bar.ssl.verification_mode", VerificationMode.CERTIFICATE);
        }
        globalSettings = builder.build();
        Environment environment = new Environment(globalSettings);
        sslService = new SSLService(globalSettings, environment);
    }

    public void testConnect() throws Exception {
        //openldap does not use cn as naming attributes by default
        String groupSearchBase = "ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        String userTemplate = "uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        RealmConfig config = new RealmConfig("oldap-test", buildLdapSettings(OPEN_LDAP_URL, userTemplate, groupSearchBase,
                LdapSearchScope.ONE_LEVEL), globalSettings);
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, sslService);

        String[] users = new String[] { "blackwidow", "cap", "hawkeye", "hulk", "ironman", "thor" };
        for (String user : users) {
            try (LdapSession ldap = session(sessionFactory, user, SecuredStringTests.build(PASSWORD))) {
                assertThat(groups(ldap), hasItem(containsString("Avengers")));
            }
        }
    }

    public void testGroupSearchScopeBase() throws Exception {
        //base search on a groups means that the user can be in just one group

        String groupSearchBase = "cn=Avengers,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        String userTemplate = "uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        RealmConfig config = new RealmConfig("oldap-test", buildLdapSettings(OPEN_LDAP_URL, userTemplate, groupSearchBase,
                LdapSearchScope.BASE), globalSettings);
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, sslService);

        String[] users = new String[] { "blackwidow", "cap", "hawkeye", "hulk", "ironman", "thor" };
        for (String user : users) {
            LdapSession ldap = session(sessionFactory, user, SecuredStringTests.build(PASSWORD));
            assertThat(groups(ldap), hasItem(containsString("Avengers")));
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
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, sslService);

        try (LdapSession ldap = session(sessionFactory, "selvig", SecuredStringTests.build(PASSWORD))){
            assertThat(groups(ldap), hasItem(containsString("Geniuses")));
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/x-plugins/issues/2849")
    public void testTcpTimeout() throws Exception {
        String groupSearchBase = "ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        String userTemplate = "uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        Settings settings = Settings.builder()
                .put(buildLdapSettings(OPEN_LDAP_URL, userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put("group_search.filter", "(objectClass=*)")
                .put("ssl.verification_mode", VerificationMode.CERTIFICATE)
                .put(SessionFactory.TIMEOUT_TCP_READ_SETTING, "1ms") //1 millisecond
                .build();
        RealmConfig config = new RealmConfig("oldap-test", settings, globalSettings);
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, sslService);

        LDAPException expected = expectThrows(LDAPException.class,
                () -> session(sessionFactory, "thor", SecuredStringTests.build(PASSWORD)).groups(new PlainActionFuture<>()));
        assertThat(expected.getMessage(), containsString("A client-side timeout was encountered while waiting"));
    }

    public void testStandardLdapConnectionHostnameVerification() throws Exception {
        //openldap does not use cn as naming attributes by default
        String groupSearchBase = "ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        String userTemplate = "uid={0},ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
        Settings settings = Settings.builder()
                .put(buildLdapSettings(OPEN_LDAP_URL, userTemplate, groupSearchBase, LdapSearchScope.ONE_LEVEL))
                .put("ssl.verification_mode", VerificationMode.FULL)
                .build();

        RealmConfig config = new RealmConfig("oldap-test", settings, globalSettings);
        LdapSessionFactory sessionFactory = new LdapSessionFactory(config, sslService);

        String user = "blackwidow";
        UncategorizedExecutionException e = expectThrows(UncategorizedExecutionException.class,
                () -> session(sessionFactory, user, SecuredStringTests.build(PASSWORD)));
        assertThat(e.getCause(), instanceOf(ExecutionException.class));
        assertThat(e.getCause().getCause(), instanceOf(LDAPException.class));
        assertThat(e.getCause().getCause().getMessage(),
                anyOf(containsString("Hostname verification failed"), containsString("peer not authenticated")));
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

    protected LdapSession session(SessionFactory factory, String username, SecuredString password) {
        PlainActionFuture<LdapSession> future = new PlainActionFuture<>();
        factory.session(username, password, future);
        return future.actionGet();
    }

    protected List<String> groups(LdapSession ldapSession) {
        PlainActionFuture<List<String>> future = new PlainActionFuture<>();
        ldapSession.groups(future);
        return future.actionGet();
    }
}
