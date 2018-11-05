/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap.support;

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPConnectionPool;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPInterface;
import com.unboundid.ldap.sdk.LDAPURL;
import com.unboundid.ldap.sdk.SimpleBindRequest;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapSessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapLoadBalancingSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.support.DnRoleMapperSettings;
import org.elasticsearch.xpack.core.ssl.VerificationMode;
import org.elasticsearch.xpack.security.authc.support.DnRoleMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings.HOSTNAME_VERIFICATION_SETTING;
import static org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings.URLS_SETTING;

public abstract class LdapTestCase extends ESTestCase {

    private static final String USER_DN_TEMPLATES_SETTING_KEY = LdapSessionFactorySettings.USER_DN_TEMPLATES_SETTING.getKey();

    static int numberOfLdapServers;
    protected InMemoryDirectoryServer[] ldapServers;

    @BeforeClass
    public static void setNumberOfLdapServers() {
        numberOfLdapServers = randomIntBetween(1, 4);
    }

    @Before
    public void startLdap() throws Exception {
        ldapServers = new InMemoryDirectoryServer[numberOfLdapServers];
        for (int i = 0; i < numberOfLdapServers; i++) {
            InMemoryDirectoryServer ldapServer = new InMemoryDirectoryServer("o=sevenSeas");
            ldapServer.add("o=sevenSeas", new Attribute("dc", "UnboundID"),
                    new Attribute("objectClass", "top", "domain", "extensibleObject"));
            ldapServer.importFromLDIF(false,
                    getDataPath("/org/elasticsearch/xpack/security/authc/ldap/support/seven-seas.ldif").toString());
            // Must have privileged access because underlying server will accept socket connections
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                ldapServer.startListening();
                return null;
            });
            ldapServers[i] = ldapServer;
        }
    }

    @After
    public void stopLdap() throws Exception {
        for (int i = 0; i < numberOfLdapServers; i++) {
            ldapServers[i].shutDown(true);
        }
    }

    protected String[] ldapUrls() throws LDAPException {
        List<String> urls = new ArrayList<>(numberOfLdapServers);
        for (int i = 0; i < numberOfLdapServers; i++) {
            LDAPURL url = new LDAPURL("ldap", "localhost", ldapServers[i].getListenPort(), null, null, null, null);
            urls.add(url.toString());
        }
        return urls.toArray(Strings.EMPTY_ARRAY);
    }

    public static Settings buildLdapSettings(String ldapUrl, String userTemplate, String groupSearchBase, LdapSearchScope scope) {
        return buildLdapSettings(new String[] { ldapUrl }, new String[] { userTemplate }, groupSearchBase, scope);
    }

    public static Settings buildLdapSettings(String[] ldapUrl, String userTemplate, String groupSearchBase, LdapSearchScope scope) {
        return buildLdapSettings(ldapUrl, new String[] { userTemplate }, groupSearchBase, scope);
    }

    public static Settings buildLdapSettings(String[] ldapUrl, String[] userTemplate, String groupSearchBase, LdapSearchScope scope) {
        return buildLdapSettings(ldapUrl, userTemplate, groupSearchBase, scope, null);
    }

    public static Settings buildLdapSettings(String[] ldapUrl, String[] userTemplate,
                                             String groupSearchBase, LdapSearchScope scope,
                                             LdapLoadBalancing serverSetType) {
        return buildLdapSettings(ldapUrl, userTemplate, groupSearchBase, scope,
                serverSetType, false);
    }

    public static Settings buildLdapSettings(String[] ldapUrl, String[] userTemplate,
                                             String groupSearchBase, LdapSearchScope scope,
                                             LdapLoadBalancing serverSetType,
                                             boolean ignoreReferralErrors) {
        Settings.Builder builder = Settings.builder()
                .putList(URLS_SETTING, ldapUrl)
                .putList(USER_DN_TEMPLATES_SETTING_KEY, userTemplate)
                .put(SessionFactorySettings.TIMEOUT_TCP_CONNECTION_SETTING, TimeValue.timeValueSeconds(1L))
                .put(SessionFactorySettings.IGNORE_REFERRAL_ERRORS_SETTING.getKey(), ignoreReferralErrors)
                .put("group_search.base_dn", groupSearchBase)
                .put("group_search.scope", scope);
        if (serverSetType != null) {
            builder.put(LdapLoadBalancingSettings.LOAD_BALANCE_SETTINGS + "." +
                            LdapLoadBalancingSettings.LOAD_BALANCE_TYPE_SETTING, serverSetType.toString());
        }
        return builder.build();
    }

    public static Settings buildLdapSettings(String[] ldapUrl, String userTemplate, boolean hostnameVerification) {
        Settings.Builder builder = Settings.builder()
                .putList(URLS_SETTING, ldapUrl)
                .putList(USER_DN_TEMPLATES_SETTING_KEY, userTemplate);
        if (randomBoolean()) {
            builder.put("ssl.verification_mode", hostnameVerification ? VerificationMode.FULL : VerificationMode.CERTIFICATE);
        } else {
            builder.put(HOSTNAME_VERIFICATION_SETTING, hostnameVerification);
        }
        return builder.build();
    }

    protected DnRoleMapper buildGroupAsRoleMapper(ResourceWatcherService resourceWatcherService) {
        Settings settings = Settings.builder()
                .put(DnRoleMapperSettings.USE_UNMAPPED_GROUPS_AS_ROLES_SETTING.getKey(), true)
                .build();
        Settings global = Settings.builder().put("path.home", createTempDir()).build();
        RealmConfig config = new RealmConfig("ldap1", settings, global, TestEnvironment.newEnvironment(global),
                new ThreadContext(Settings.EMPTY));

        return new DnRoleMapper(config, resourceWatcherService);
    }

    protected LdapSession session(SessionFactory factory, String username, SecureString password) {
        PlainActionFuture<LdapSession> future = new PlainActionFuture<>();
        factory.session(username, password, future);
        return future.actionGet();
    }

    protected List<String> groups(LdapSession ldapSession) {
        Objects.requireNonNull(ldapSession);
        PlainActionFuture<List<String>> future = new PlainActionFuture<>();
        ldapSession.groups(future);
        return future.actionGet();
    }

    protected LdapSession unauthenticatedSession(SessionFactory factory, String username) {
        PlainActionFuture<LdapSession> future = new PlainActionFuture<>();
        factory.unauthenticatedSession(username, future);
        return future.actionGet();
    }

    protected static void assertConnectionValid(LDAPInterface conn, SimpleBindRequest bindRequest) {
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                try {
                    if (conn instanceof LDAPConnection) {
                        assertTrue(((LDAPConnection) conn).isConnected());
                        assertEquals(bindRequest.getBindDN(),
                                ((SimpleBindRequest)((LDAPConnection) conn).getLastBindRequest()).getBindDN());
                        ((LDAPConnection) conn).reconnect();
                    } else if (conn instanceof LDAPConnectionPool) {
                        try (LDAPConnection c = ((LDAPConnectionPool) conn).getConnection()) {
                            assertTrue(c.isConnected());
                            assertEquals(bindRequest.getBindDN(), ((SimpleBindRequest)c.getLastBindRequest()).getBindDN());
                            c.reconnect();
                        }
                    }
                } catch (LDAPException e) {
                    fail("Connection is not valid. It will not work on follow referral flow." +
                            System.lineSeparator() + ExceptionsHelper.stackTrace(e));
                }
                return null;
            }
        });
    }
}
