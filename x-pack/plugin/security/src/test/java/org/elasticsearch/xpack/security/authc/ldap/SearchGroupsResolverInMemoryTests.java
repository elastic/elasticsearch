/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPConnectionOptions;
import com.unboundid.ldap.sdk.LDAPConnectionPool;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPURL;
import com.unboundid.ldap.sdk.ResultCode;
import com.unboundid.ldap.sdk.SimpleBindRequest;
import com.unboundid.ldap.sdk.SingleServerSet;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.ldap.SearchGroupsResolverSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapTestCase;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils;
import org.junit.After;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;

public class SearchGroupsResolverInMemoryTests extends LdapTestCase {

    private static final String WILLIAM_BUSH = "cn=William Bush,ou=people,o=sevenSeas";
    public static final RealmConfig.RealmIdentifier REALM_IDENTIFIER = new RealmConfig.RealmIdentifier("ldap", "ldap1");
    private LDAPConnection connection;

    @After
    public void closeConnection() {
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * Tests that a client-side timeout in the asynchronous LDAP SDK is treated as a failure, rather
     * than simply returning no results.
     */
    public void testSearchTimeoutIsFailure() throws Exception {
        ldapServers[0].setProcessingDelayMillis(500);

        final LDAPConnectionOptions options = new LDAPConnectionOptions();
        options.setConnectTimeoutMillis(1500);
        options.setResponseTimeoutMillis(5);
        connect(options);

        final Settings settings = Settings.builder()
                .put(getFullSettingKey(REALM_IDENTIFIER, SearchGroupsResolverSettings.BASE_DN), "ou=groups,o=sevenSeas")
                .put(getFullSettingKey(REALM_IDENTIFIER, SearchGroupsResolverSettings.SCOPE), LdapSearchScope.SUB_TREE)
                .build();
        final SearchGroupsResolver resolver = new SearchGroupsResolver(getConfig(settings));
        final PlainActionFuture<List<String>> future = new PlainActionFuture<>();
        resolver.resolve(connection, WILLIAM_BUSH, TimeValue.timeValueSeconds(30), logger, null, future);

        final ExecutionException exception = expectThrows(ExecutionException.class, future::get);
        final Throwable cause = exception.getCause();
        assertThat(cause, instanceOf(LDAPException.class));
        assertThat(((LDAPException) cause).getResultCode(), is(ResultCode.TIMEOUT));
    }

    /**
     * Tests searching for groups when the "user_attribute" field is not set
     */
    public void testResolveWithDefaultUserAttribute() throws Exception {
        connect(new LDAPConnectionOptions());

        Settings settings = Settings.builder()
                .put(getFullSettingKey(REALM_IDENTIFIER, SearchGroupsResolverSettings.BASE_DN), "ou=groups,o=sevenSeas")
                .put(getFullSettingKey(REALM_IDENTIFIER, SearchGroupsResolverSettings.SCOPE), LdapSearchScope.SUB_TREE)
                .build();

        final List<String> groups = resolveGroups(settings, WILLIAM_BUSH);
        assertThat(groups, iterableWithSize(1));
        assertThat(groups.get(0), containsString("HMS Lydia"));
    }

    /**
     * Tests searching for groups when the "user_attribute" field is set to "dn" (which is special)
     */
    public void testResolveWithExplicitDnAttribute() throws Exception {
        connect(new LDAPConnectionOptions());

        Settings settings = Settings.builder()
                .put(getFullSettingKey(REALM_IDENTIFIER, SearchGroupsResolverSettings.BASE_DN), "ou=groups,o=sevenSeas")
                .put(getFullSettingKey(REALM_IDENTIFIER.getName(), SearchGroupsResolverSettings.USER_ATTRIBUTE), "dn")
                .build();

        final List<String> groups = resolveGroups(settings, WILLIAM_BUSH);
        assertThat(groups, iterableWithSize(1));
        assertThat(groups.get(0), containsString("HMS Lydia"));
    }

    /**
     * Tests searching for groups when the "user_attribute" field is set to a missing value
     */
    public void testResolveWithMissingAttribute() throws Exception {
        connect(new LDAPConnectionOptions());

        Settings settings = Settings.builder()
                .put(getFullSettingKey(REALM_IDENTIFIER, SearchGroupsResolverSettings.BASE_DN), "ou=groups,o=sevenSeas")
                .put(getFullSettingKey(REALM_IDENTIFIER.getName(), SearchGroupsResolverSettings.USER_ATTRIBUTE), "no-such-attribute")
                .build();

        final List<String> groups = resolveGroups(settings, WILLIAM_BUSH);
        assertThat(groups, iterableWithSize(0));
    }

    public void testSearchWithConnectionPoolForOneResult() throws Exception {
        final LDAPURL ldapurl = new LDAPURL(ldapUrls()[0]);

        try (LDAPConnectionPool pool =
                     LdapUtils.privilegedConnect(() -> new LDAPConnectionPool(new SingleServerSet(ldapurl.getHost(), ldapurl.getPort()),
                             new SimpleBindRequest("cn=Horatio Hornblower,ou=people,o=sevenSeas", "pass"), 0, 20))) {

            final Settings settings = Settings.builder()
                    .put(getFullSettingKey(REALM_IDENTIFIER, PoolingSessionFactorySettings.BIND_DN),
                            "cn=Horatio Hornblower,ou=people,o=sevenSeas")
                    .put(getFullSettingKey(REALM_IDENTIFIER, PoolingSessionFactorySettings.LEGACY_BIND_PASSWORD), "pass")
                    .put(getFullSettingKey(REALM_IDENTIFIER, SearchGroupsResolverSettings.BASE_DN), "ou=groups,o=sevenSeas")
                    .put(getFullSettingKey(REALM_IDENTIFIER, SearchGroupsResolverSettings.SCOPE), LdapSearchScope.SUB_TREE)
                    .build();
            final SearchGroupsResolver resolver = new SearchGroupsResolver(getConfig(settings));
            final PlainActionFuture<List<String>> future = new PlainActionFuture<>();
            resolver.resolve(pool,
                    "cn=Moultrie Crystal,ou=people,o=sevenSeas",
                    TimeValue.timeValueSeconds(30),
                    logger,
                    null, future);
            List<String> resolvedDNs = future.actionGet();
            assertEquals(1, resolvedDNs.size());
        }
    }

    private void connect(LDAPConnectionOptions options) throws LDAPException {
        if (connection != null) {
            throw new IllegalStateException("Already connected (" + connection.getConnectionName() + ' '
                    + connection.getConnectedAddress() + ')');
        }
        final LDAPURL ldapurl = new LDAPURL(ldapUrls()[0]);
        this.connection = LdapUtils.privilegedConnect(() -> new LDAPConnection(options, ldapurl.getHost(), ldapurl.getPort()));
    }

    private List<String> resolveGroups(Settings settings, String userDn) {
        final SearchGroupsResolver resolver = new SearchGroupsResolver(getConfig(settings));
        final PlainActionFuture<List<String>> future = new PlainActionFuture<>();
        resolver.resolve(connection, userDn, TimeValue.timeValueSeconds(30), logger, null, future);
        return future.actionGet();
    }

    private RealmConfig getConfig(Settings settings) {
        if (settings.hasValue("path.home") == false) {
            settings = Settings.builder().put(settings).put("path.home", createTempDir()).build();
        }
        return new RealmConfig(REALM_IDENTIFIER,
            Settings.builder().put(settings).put(getFullSettingKey(REALM_IDENTIFIER, RealmSettings.ORDER_SETTING), 0).build(),
            TestEnvironment.newEnvironment(settings), new ThreadContext(settings));
    }

}
