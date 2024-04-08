/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;
import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.FailoverServerSet;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPURL;
import com.unboundid.ldap.sdk.SingleServerSet;
import com.unboundid.ldap.sdk.schema.Schema;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslVerificationMode;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.ActiveDirectorySessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapLoadBalancingSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapMetadataResolverSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.support.CachingUsernamePasswordRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.DnRoleMapperSettings;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.TemplateRoleName;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.ldap.ActiveDirectorySessionFactory.DownLevelADAuthenticator;
import org.elasticsearch.xpack.security.authc.ldap.ActiveDirectorySessionFactory.UpnADAuthenticator;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapServerDebugLogging;
import org.elasticsearch.xpack.security.authc.support.DnRoleMapper;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestRule;

import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings.URLS_SETTING;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Active Directory Realm tests that use the UnboundID In Memory Directory Server
 * <p>
 * AD is not LDAPv3 compliant so a workaround is needed
 * AD realm binds with userPrincipalName but this is not a valid DN, so we have to add a second userPrincipalName to the
 * users in the ldif in the form of CN=user@domain.com or a set the sAMAccountName to CN=user when testing authentication
 * with the sAMAccountName field.
 * <p>
 * The username used to authenticate then has to be in the form of CN=user. Finally the username needs to be added as an
 * additional bind DN with a password in the test setup since it really is not a DN in the ldif file
 */
public class ActiveDirectoryRealmTests extends ESTestCase {

    private static final String PASSWORD = "password";

    static int numberOfLdapServers;
    InMemoryDirectoryServer[] directoryServers;

    private ResourceWatcherService resourceWatcherService;
    private ThreadPool threadPool;
    private Settings globalSettings;
    private SSLService sslService;
    private XPackLicenseState licenseState;
    private LdapServerDebugLogging debugLogging = new LdapServerDebugLogging(logger);

    @BeforeClass
    public static void setNumberOfLdapServers() {
        numberOfLdapServers = randomIntBetween(1, 4);
    }

    @Rule
    public TestRule printLdapDebugOnFailure = debugLogging.getTestWatcher();

    @Before
    public void start() throws Exception {
        InMemoryDirectoryServerConfig config = new InMemoryDirectoryServerConfig("dc=ad,dc=test,dc=elasticsearch,dc=com");
        debugLogging.configure(config);

        // Get the default schema and overlay with the AD changes
        config.setSchema(
            Schema.mergeSchemas(Schema.getDefaultStandardSchema(), Schema.getSchema(getDataPath("ad-schema.ldif").toString()))
        );

        // Add the bind users here since AD is not LDAPv3 compliant
        config.addAdditionalBindCredentials("CN=ironman@ad.test.elasticsearch.com", PASSWORD);
        config.addAdditionalBindCredentials("CN=Thor@ad.test.elasticsearch.com", PASSWORD);

        directoryServers = new InMemoryDirectoryServer[numberOfLdapServers];
        for (int i = 0; i < numberOfLdapServers; i++) {
            InMemoryDirectoryServer directoryServer = new InMemoryDirectoryServer(config);
            directoryServer.add(
                "dc=ad,dc=test,dc=elasticsearch,dc=com",
                new Attribute("dc", "UnboundID"),
                new Attribute("objectClass", "top", "domain", "extensibleObject")
            );
            directoryServer.importFromLDIF(false, getDataPath("ad.ldif").toString());
            // Must have privileged access because underlying server will accept socket connections
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                directoryServer.startListening();
                return null;
            });
            directoryServers[i] = directoryServer;
        }
        threadPool = new TestThreadPool("active directory realm tests");
        resourceWatcherService = new ResourceWatcherService(Settings.EMPTY, threadPool);
        globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        sslService = new SSLService(TestEnvironment.newEnvironment(globalSettings));
        licenseState = new TestUtils.UpdatableLicenseState();

        // Verify we can connect to each server. Tests will fail in strange ways if this isn't true
        Arrays.stream(directoryServers).forEachOrdered(ds -> tryConnect(ds));
    }

    private void tryConnect(InMemoryDirectoryServer ds) {
        try {
            AccessController.doPrivileged((PrivilegedExceptionAction<Void>) () -> {
                try (var c = ds.getConnection()) {
                    assertThat("Failed to connect to " + ds, c.isConnected(), is(true));
                } catch (LDAPException e) {
                    throw new AssertionError("Failed to connect to " + ds, e);
                }
                return null;
            });
        } catch (PrivilegedActionException e) {
            throw new AssertionError("Failed to connect to " + ds, e);
        }
    }

    @After
    public void stop() throws InterruptedException {
        resourceWatcherService.close();
        terminate(threadPool);
        for (int i = 0; i < numberOfLdapServers; i++) {
            directoryServers[i].shutDown(true);
        }
    }

    @Override
    public boolean enableWarningsCheck() {
        return false;
    }

    /**
     * Creates a realm with the provided settings, rebuilds the SSL Service to be aware of the new realm, and then returns
     * the RealmConfig
     */
    private RealmConfig setupRealm(RealmConfig.RealmIdentifier realmIdentifier, Settings localSettings) {
        final Settings mergedSettings = Settings.builder()
            .put(globalSettings)
            .put(localSettings)
            .put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), "0")
            .build();
        final Environment env = TestEnvironment.newEnvironment(mergedSettings);
        this.sslService = new SSLService(env);
        return new RealmConfig(realmIdentifier, mergedSettings, env, new ThreadContext(mergedSettings));
    }

    public void testAuthenticateUserPrincipalName() throws Exception {
        final RealmConfig.RealmIdentifier realmIdentifier = realmId("testAuthenticateUserPrincipalName");
        Settings settings = settings(realmIdentifier);
        RealmConfig config = setupRealm(realmIdentifier, settings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService, threadPool);
        DnRoleMapper roleMapper = new DnRoleMapper(config, resourceWatcherService);
        LdapRealm realm = new LdapRealm(config, sessionFactory, roleMapper, threadPool);
        realm.initialize(Collections.singleton(realm), licenseState);

        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        final UsernamePasswordToken token = new UsernamePasswordToken("CN=ironman", new SecureString(PASSWORD));
        logger.info("Attempting to authentication with [{}]", token);
        realm.authenticate(token, future);
        final User user = getAndVerifyAuthUser(future);
        assertThat(user.roles(), arrayContaining(containsString("Avengers")));
    }

    public void testAuthenticateSAMAccountName() throws Exception {
        final RealmConfig.RealmIdentifier realmIdentifier = realmId("testAuthenticateSAMAccountName");
        Settings settings = settings(realmIdentifier);
        RealmConfig config = setupRealm(realmIdentifier, settings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService, threadPool);
        DnRoleMapper roleMapper = new DnRoleMapper(config, resourceWatcherService);
        LdapRealm realm = new LdapRealm(config, sessionFactory, roleMapper, threadPool);
        realm.initialize(Collections.singleton(realm), licenseState);

        // Thor does not have a UPN of form CN=Thor@ad.test.elasticsearch.com
        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("CN=Thor", new SecureString(PASSWORD)), future);
        final User user = getAndVerifyAuthUser(future);
        assertThat(user.roles(), arrayContaining(containsString("Avengers")));
    }

    protected String[] ldapUrls() throws LDAPException {
        List<String> urls = new ArrayList<>(numberOfLdapServers);
        for (int i = 0; i < numberOfLdapServers; i++) {
            LDAPURL url = new LDAPURL("ldap", "localhost", directoryServers[i].getListenPort(), null, null, null, null);
            urls.add(url.toString());
        }
        return urls.toArray(Strings.EMPTY_ARRAY);
    }

    public void testAuthenticateCachesSuccessfulAuthentications() throws Exception {
        final RealmConfig.RealmIdentifier realmIdentifier = realmId("testAuthenticateCachesSuccesfulAuthentications");
        Settings settings = settings(realmIdentifier);
        RealmConfig config = setupRealm(realmIdentifier, settings);
        ActiveDirectorySessionFactory sessionFactory = spy(new ActiveDirectorySessionFactory(config, sslService, threadPool));
        DnRoleMapper roleMapper = new DnRoleMapper(config, resourceWatcherService);
        LdapRealm realm = new LdapRealm(config, sessionFactory, roleMapper, threadPool);
        realm.initialize(Collections.singleton(realm), licenseState);

        int count = randomIntBetween(2, 10);
        for (int i = 0; i < count; i++) {
            PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
            realm.authenticate(new UsernamePasswordToken("CN=ironman", new SecureString(PASSWORD)), future);
            future.actionGet();
        }

        // verify one and only one session as further attempts should be returned from cache
        verify(sessionFactory, times(1)).session(eq("CN=ironman"), any(SecureString.class), anyActionListener());
    }

    public void testAuthenticateCachingCanBeDisabled() throws Exception {
        final RealmConfig.RealmIdentifier realmIdentifier = realmId("testAuthenticateCachingCanBeDisabled");
        final Settings settings = settings(
            realmIdentifier,
            Settings.builder().put(getFullSettingKey(realmIdentifier, CachingUsernamePasswordRealmSettings.CACHE_TTL_SETTING), -1).build()
        );
        RealmConfig config = setupRealm(realmIdentifier, settings);
        ActiveDirectorySessionFactory sessionFactory = spy(new ActiveDirectorySessionFactory(config, sslService, threadPool));
        DnRoleMapper roleMapper = new DnRoleMapper(config, resourceWatcherService);
        LdapRealm realm = new LdapRealm(config, sessionFactory, roleMapper, threadPool);
        realm.initialize(Collections.singleton(realm), licenseState);

        int count = randomIntBetween(2, 10);
        for (int i = 0; i < count; i++) {
            PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
            realm.authenticate(new UsernamePasswordToken("CN=ironman", new SecureString(PASSWORD)), future);
            future.actionGet();
        }

        // verify one and only one session as second attempt should be returned from cache
        verify(sessionFactory, times(count)).session(eq("CN=ironman"), any(SecureString.class), anyActionListener());
    }

    public void testAuthenticateCachingClearsCacheOnRoleMapperRefresh() throws Exception {
        final RealmConfig.RealmIdentifier realmIdentifier = realmId("testAuthenticateCachingClearsCacheOnRoleMapperRefresh");
        Settings settings = settings(realmIdentifier);
        RealmConfig config = setupRealm(realmIdentifier, settings);
        ActiveDirectorySessionFactory sessionFactory = spy(new ActiveDirectorySessionFactory(config, sslService, threadPool));
        DnRoleMapper roleMapper = new DnRoleMapper(config, resourceWatcherService);
        LdapRealm realm = new LdapRealm(config, sessionFactory, roleMapper, threadPool);
        realm.initialize(Collections.singleton(realm), licenseState);

        int count = randomIntBetween(2, 10);
        for (int i = 0; i < count; i++) {
            PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
            realm.authenticate(new UsernamePasswordToken("CN=ironman", new SecureString(PASSWORD)), future);
            final AuthenticationResult<User> result = future.actionGet();
            assertThat("Authentication result: " + result, result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        }

        // verify one and only one session as further attempts should be returned from cache
        verify(sessionFactory, times(1)).session(eq("CN=ironman"), any(SecureString.class), anyActionListener());

        // Refresh the role mappings
        roleMapper.notifyRefresh();

        for (int i = 0; i < count; i++) {
            PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
            realm.authenticate(new UsernamePasswordToken("CN=ironman", new SecureString(PASSWORD)), future);
            future.actionGet();
        }

        verify(sessionFactory, times(2)).session(eq("CN=ironman"), any(SecureString.class), anyActionListener());
    }

    public void testUnauthenticatedLookupWithConnectionPool() throws Exception {
        doUnauthenticatedLookup(true);
    }

    public void testUnauthenticatedLookupWithoutConnectionPool() throws Exception {
        doUnauthenticatedLookup(false);
    }

    private void doUnauthenticatedLookup(boolean pooled) throws Exception {
        final RealmConfig.RealmIdentifier realmIdentifier = realmId("testUnauthenticatedLookupWithConnectionPool");

        final Settings.Builder builder = Settings.builder()
            .put(getFullSettingKey(realmIdentifier.getName(), ActiveDirectorySessionFactorySettings.POOL_ENABLED), pooled)
            .put(getFullSettingKey(realmIdentifier, PoolingSessionFactorySettings.BIND_DN), "CN=ironman@ad.test.elasticsearch.com");
        final boolean useLegacyBindPassword = randomBoolean();
        if (useLegacyBindPassword) {
            builder.put(getFullSettingKey(realmIdentifier, PoolingSessionFactorySettings.LEGACY_BIND_PASSWORD), PASSWORD);
        } else {
            final MockSecureSettings secureSettings = new MockSecureSettings();
            secureSettings.setString(getFullSettingKey(realmIdentifier, PoolingSessionFactorySettings.SECURE_BIND_PASSWORD), PASSWORD);
            builder.setSecureSettings(secureSettings);
        }
        Settings settings = settings(realmIdentifier, builder.build());
        RealmConfig config = setupRealm(realmIdentifier, settings);
        try (ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService, threadPool)) {
            DnRoleMapper roleMapper = new DnRoleMapper(config, resourceWatcherService);
            LdapRealm realm = new LdapRealm(config, sessionFactory, roleMapper, threadPool);
            realm.initialize(Collections.singleton(realm), licenseState);

            PlainActionFuture<User> future = new PlainActionFuture<>();
            realm.lookupUser("CN=Thor", future);
            final User user = future.actionGet();
            assertThat(user, notNullValue());
            assertThat(user.principal(), equalTo("CN=Thor"));
        }
    }

    public void testRealmMapsGroupsToRoles() throws Exception {
        final RealmConfig.RealmIdentifier realmId = realmId("testRealmMapsGroupsToRoles");
        Settings settings = settings(
            realmId,
            Settings.builder()
                .put(getFullSettingKey(realmId, DnRoleMapperSettings.ROLE_MAPPING_FILE_SETTING), getDataPath("role_mapping.yml"))
                .build()
        );
        RealmConfig config = setupRealm(realmId, settings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService, threadPool);
        DnRoleMapper roleMapper = new DnRoleMapper(config, resourceWatcherService);
        LdapRealm realm = new LdapRealm(config, sessionFactory, roleMapper, threadPool);
        realm.initialize(Collections.singleton(realm), licenseState);

        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("CN=ironman", new SecureString(PASSWORD)), future);
        final User user = getAndVerifyAuthUser(future);
        assertThat(user.roles(), arrayContaining(equalTo("group_role")));
    }

    public void testRealmMapsUsersToRoles() throws Exception {
        final RealmConfig.RealmIdentifier realmId = realmId("testRealmMapsGroupsToRoles");
        Settings settings = settings(
            realmId,
            Settings.builder()
                .put(getFullSettingKey(realmId, DnRoleMapperSettings.ROLE_MAPPING_FILE_SETTING), getDataPath("role_mapping.yml"))
                .build()
        );
        RealmConfig config = setupRealm(realmId, settings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService, threadPool);
        DnRoleMapper roleMapper = new DnRoleMapper(config, resourceWatcherService);
        LdapRealm realm = new LdapRealm(config, sessionFactory, roleMapper, threadPool);
        realm.initialize(Collections.singleton(realm), licenseState);

        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("CN=Thor", new SecureString(PASSWORD)), future);
        final User user = getAndVerifyAuthUser(future);
        assertThat(user.roles(), arrayContainingInAnyOrder(equalTo("group_role"), equalTo("user_role")));
    }

    /**
     * This tests template role mappings (see
     * {@link TemplateRoleName}) with an LDAP realm, using a additional
     * metadata field (see {@link LdapMetadataResolverSettings#ADDITIONAL_METADATA_SETTING}).
     */
    public void testRealmWithTemplatedRoleMapping() throws Exception {
        final RealmConfig.RealmIdentifier realmId = realmId("testRealmWithTemplatedRoleMapping");
        Settings settings = settings(
            realmId,
            Settings.builder()
                .put(getFullSettingKey(realmId, LdapMetadataResolverSettings.ADDITIONAL_METADATA_SETTING), "departmentNumber")
                .build()
        );
        RealmConfig config = setupRealm(realmId, settings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService, threadPool);

        SecurityIndexManager mockSecurityIndex = mock(SecurityIndexManager.class);
        when(mockSecurityIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(true);
        when(mockSecurityIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(true);
        when(mockSecurityIndex.isIndexUpToDate()).thenReturn(true);
        when(mockSecurityIndex.indexExists()).thenReturn(true);
        when(mockSecurityIndex.defensiveCopy()).thenReturn(mockSecurityIndex);

        Client mockClient = mock(Client.class);
        when(mockClient.threadPool()).thenReturn(threadPool);

        final ScriptService scriptService = new ScriptService(
            settings,
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine()),
            ScriptModule.CORE_CONTEXTS,
            () -> 1L
        );
        NativeRoleMappingStore roleMapper = new NativeRoleMappingStore(settings, mockClient, mockSecurityIndex, scriptService) {
            @Override
            protected void loadMappings(ActionListener<List<ExpressionRoleMapping>> listener) {
                listener.onResponse(Arrays.asList(NativeRoleMappingStore.buildMapping("m1", new BytesArray("""
                    {
                      "role_templates": [ { "template": { "source": "_role_{{metadata.departmentNumber}}" } } ],
                      "enabled": true,
                      "rules": {
                        "field": {
                          "realm.name": "testrealmwithtemplatedrolemapping"
                        }
                      }
                    }"""))));
            }
        };
        LdapRealm realm = new LdapRealm(config, sessionFactory, roleMapper, threadPool);
        realm.initialize(Collections.singleton(realm), licenseState);

        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("CN=Thor", new SecureString(PASSWORD)), future);
        User user = getAndVerifyAuthUser(future);
        assertThat(user.roles(), arrayContaining("_role_13"));

        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("CN=ironman", new SecureString(PASSWORD)), future);
        user = getAndVerifyAuthUser(future);
        assertThat(user.roles(), arrayContaining("_role_12"));
    }

    public void testRealmUsageStats() throws Exception {
        final RealmConfig.RealmIdentifier realmId = realmId("testRealmUsageStats");
        String loadBalanceType = randomFrom("failover", "round_robin");
        Settings settings = settings(
            realmId,
            Settings.builder()
                .put(getFullSettingKey(realmId, DnRoleMapperSettings.ROLE_MAPPING_FILE_SETTING), getDataPath("role_mapping.yml"))
                .put(getFullSettingKey(realmId, LdapLoadBalancingSettings.LOAD_BALANCE_TYPE_SETTING), loadBalanceType)
                .build()
        );
        RealmConfig config = setupRealm(realmId, settings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService, threadPool);
        DnRoleMapper roleMapper = new DnRoleMapper(config, resourceWatcherService);
        LdapRealm realm = new LdapRealm(config, sessionFactory, roleMapper, threadPool);
        realm.initialize(Collections.singleton(realm), licenseState);

        PlainActionFuture<Map<String, Object>> future = new PlainActionFuture<>();
        realm.usageStats(future);
        Map<String, Object> stats = future.get();
        assertThat(stats, is(notNullValue()));
        assertThat(stats, hasEntry("name", realm.name()));
        assertThat(stats, hasEntry("order", realm.order()));
        assertThat(stats, hasEntry("size", 0));
        assertThat(stats, hasEntry("ssl", false));
        assertThat(stats, hasEntry("load_balance_type", loadBalanceType));
    }

    public void testDefaultSearchFilters() throws Exception {
        final RealmConfig.RealmIdentifier realmIdentifier = realmId("testDefaultSearchFilters");
        Settings settings = settings(realmIdentifier);
        RealmConfig config = setupRealm(realmIdentifier, settings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService, threadPool);
        assertEquals(
            "(&(objectClass=user)(|(sAMAccountName={0})(userPrincipalName={0}@ad.test.elasticsearch.com)))",
            sessionFactory.defaultADAuthenticator.getUserSearchFilter()
        );
        assertEquals(UpnADAuthenticator.UPN_USER_FILTER, sessionFactory.upnADAuthenticator.getUserSearchFilter());
        assertEquals(DownLevelADAuthenticator.DOWN_LEVEL_FILTER, sessionFactory.downLevelADAuthenticator.getUserSearchFilter());
    }

    public void testCustomSearchFilters() throws Exception {
        final RealmConfig.RealmIdentifier realmId = realmId("testDefaultSearchFilters");
        Settings settings = settings(
            realmId,
            Settings.builder()
                .put(
                    getFullSettingKey(realmId.getName(), ActiveDirectorySessionFactorySettings.AD_USER_SEARCH_FILTER_SETTING),
                    "(objectClass=default)"
                )
                .put(
                    getFullSettingKey(realmId.getName(), ActiveDirectorySessionFactorySettings.AD_UPN_USER_SEARCH_FILTER_SETTING),
                    "(objectClass=upn)"
                )
                .put(
                    getFullSettingKey(realmId.getName(), ActiveDirectorySessionFactorySettings.AD_DOWN_LEVEL_USER_SEARCH_FILTER_SETTING),
                    "(objectClass=down level)"
                )
                .build()
        );
        RealmConfig config = setupRealm(realmId, settings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService, threadPool);
        assertEquals("(objectClass=default)", sessionFactory.defaultADAuthenticator.getUserSearchFilter());
        assertEquals("(objectClass=upn)", sessionFactory.upnADAuthenticator.getUserSearchFilter());
        assertEquals("(objectClass=down level)", sessionFactory.downLevelADAuthenticator.getUserSearchFilter());
    }

    public RealmConfig.RealmIdentifier realmId(String realmName) {
        return new RealmConfig.RealmIdentifier(LdapRealmSettings.AD_TYPE, realmName.toLowerCase(Locale.ROOT));
    }

    private Settings settings(RealmConfig.RealmIdentifier realmIdentifier) throws Exception {
        return settings(realmIdentifier, Settings.EMPTY);
    }

    public void testBuildUrlFromDomainNameAndDefaultPort() throws Exception {
        final RealmConfig.RealmIdentifier realmId = realmId("testBuildUrlFromDomainNameAndDefaultPort");
        Settings settings = Settings.builder()
            .put(getFullSettingKey(realmId, ActiveDirectorySessionFactorySettings.AD_DOMAIN_NAME_SETTING), "ad.test.elasticsearch.com")
            .build();
        RealmConfig config = setupRealm(realmId, settings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService, threadPool);
        assertSingleLdapServer(sessionFactory, "ad.test.elasticsearch.com", 389);
    }

    public void testBuildUrlFromDomainNameAndCustomPort() throws Exception {
        final RealmConfig.RealmIdentifier realmId = realmId("testBuildUrlFromDomainNameAndCustomPort");
        Settings settings = Settings.builder()
            .put(getFullSettingKey(realmId, ActiveDirectorySessionFactorySettings.AD_DOMAIN_NAME_SETTING), "ad.test.elasticsearch.com")
            .put(getFullSettingKey(realmId.getName(), ActiveDirectorySessionFactorySettings.AD_LDAP_PORT_SETTING), 10389)
            .build();
        RealmConfig config = setupRealm(realmId, settings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService, threadPool);
        assertSingleLdapServer(sessionFactory, "ad.test.elasticsearch.com", 10389);
    }

    public void testUrlConfiguredInSettings() throws Exception {
        final RealmConfig.RealmIdentifier realmId = realmId("testBuildUrlFromDomainNameAndCustomPort");
        Settings settings = Settings.builder()
            .put(getFullSettingKey(realmId, ActiveDirectorySessionFactorySettings.AD_DOMAIN_NAME_SETTING), "ad.test.elasticsearch.com")
            .put(getFullSettingKey(realmId, SessionFactorySettings.URLS_SETTING), "ldap://ad01.testing.elastic.co:20389/")
            .build();
        RealmConfig config = setupRealm(realmId, settings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService, threadPool);
        assertSingleLdapServer(sessionFactory, "ad01.testing.elastic.co", 20389);
    }

    public void testMandatorySettings() throws Exception {
        final RealmConfig.RealmIdentifier realmId = realmId("testMandatorySettingsTestRealm");
        Settings settings = Settings.builder()
            .put(getFullSettingKey(realmId, ActiveDirectorySessionFactorySettings.AD_DOMAIN_NAME_SETTING), randomBoolean() ? null : "")
            .build();
        RealmConfig config = setupRealm(realmId, settings);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new ActiveDirectorySessionFactory(config, sslService, threadPool)
        );
        assertThat(
            e.getMessage(),
            containsString(getFullSettingKey(realmId, ActiveDirectorySessionFactorySettings.AD_DOMAIN_NAME_SETTING))
        );
    }

    public void testReloadBindPassword() throws Exception {
        final RealmConfig.RealmIdentifier realmIdentifier = realmId("testReloadBindPassword");
        final boolean useLegacyBindPassword = randomBoolean();
        final boolean pooled = randomBoolean();
        final Settings.Builder builder = Settings.builder()
            .put(getFullSettingKey(realmIdentifier, PoolingSessionFactorySettings.BIND_DN), "CN=ironman@ad.test.elasticsearch.com")
            .put(getFullSettingKey(realmIdentifier.getName(), ActiveDirectorySessionFactorySettings.POOL_ENABLED), pooled)
            // explicitly disabling cache to always authenticate against AD server
            .put(getFullSettingKey(realmIdentifier, CachingUsernamePasswordRealmSettings.CACHE_TTL_SETTING), -1)
            // due to limitations of AD server we cannot change BIND password dynamically,
            // so we start with the wrong (random) password and then reload and check if authentication succeeds
            .put(bindPasswordSettings(realmIdentifier, useLegacyBindPassword, randomAlphaOfLengthBetween(3, 7)));

        Settings settings = settings(realmIdentifier, builder.build());
        RealmConfig config = setupRealm(realmIdentifier, settings);
        try (ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, sslService, threadPool)) {
            DnRoleMapper roleMapper = new DnRoleMapper(config, resourceWatcherService);
            LdapRealm realm = new LdapRealm(config, sessionFactory, roleMapper, threadPool);
            realm.initialize(Collections.singleton(realm), licenseState);

            {
                final PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
                realm.authenticate(new UsernamePasswordToken("CN=Thor", new SecureString(PASSWORD.toCharArray())), future);
                final AuthenticationResult<User> result = future.actionGet();
                assertThat(result.toString(), result.getStatus(), is(AuthenticationResult.Status.CONTINUE));
            }

            realm.reload(bindPasswordSettings(realmIdentifier, useLegacyBindPassword, PASSWORD));

            {
                final PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
                realm.authenticate(new UsernamePasswordToken("CN=Thor", new SecureString(PASSWORD.toCharArray())), future);
                final AuthenticationResult<User> result = future.actionGet();
                assertThat(result.toString(), result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
            }

            // Verify that reloading fails if password gets removed when bind dn is configured.
            var e = expectThrows(Exception.class, () -> realm.reload(Settings.EMPTY));
            assertThat(
                e.getMessage(),
                containsString(
                    "["
                        + getFullSettingKey(config, PoolingSessionFactorySettings.BIND_DN)
                        + "] is set but no bind password is specified. Without a corresponding bind password, "
                        + "all "
                        + realm.type()
                        + " realm authentication will fail. Specify a bind password via ["
                        + getFullSettingKey(config, PoolingSessionFactorySettings.SECURE_BIND_PASSWORD)
                        + "]."
                )
            );
        }
    }

    private void assertSingleLdapServer(ActiveDirectorySessionFactory sessionFactory, String hostname, int port) {
        assertThat(sessionFactory.getServerSet(), instanceOf(FailoverServerSet.class));
        FailoverServerSet fss = (FailoverServerSet) sessionFactory.getServerSet();
        assertThat(fss.getServerSets(), arrayWithSize(1));
        assertThat(fss.getServerSets()[0], instanceOf(SingleServerSet.class));
        SingleServerSet sss = (SingleServerSet) fss.getServerSets()[0];
        assertThat(sss.getAddress(), equalTo(hostname));
        assertThat(sss.getPort(), equalTo(port));
    }

    private Settings settings(RealmConfig.RealmIdentifier realmIdentifier, Settings extraSettings) throws Exception {
        Settings.Builder builder = Settings.builder()
            .putList(getFullSettingKey(realmIdentifier, URLS_SETTING), ldapUrls())
            .put(
                getFullSettingKey(realmIdentifier, ActiveDirectorySessionFactorySettings.AD_DOMAIN_NAME_SETTING),
                "ad.test.elasticsearch.com"
            )
            .put(getFullSettingKey(realmIdentifier, DnRoleMapperSettings.USE_UNMAPPED_GROUPS_AS_ROLES_SETTING), true);
        if (inFipsJvm()) {
            builder.put(
                getFullSettingKey(realmIdentifier, SSLConfigurationSettings.VERIFICATION_MODE_SETTING_REALM),
                SslVerificationMode.CERTIFICATE
            );
        } else {
            builder.put(
                getFullSettingKey(realmIdentifier, SSLConfigurationSettings.VERIFICATION_MODE_SETTING_REALM),
                randomBoolean() ? SslVerificationMode.CERTIFICATE : SslVerificationMode.NONE
            );
        }
        return builder.put(extraSettings).build();
    }

    private User getAndVerifyAuthUser(PlainActionFuture<AuthenticationResult<User>> future) {
        final AuthenticationResult<User> result = future.actionGet();
        assertThat(result.toString(), result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        final User user = result.getValue();
        assertThat(user, is(notNullValue()));
        return user;
    }

    private Settings bindPasswordSettings(RealmConfig.RealmIdentifier realmIdentifier, boolean useLegacyBindPassword, String password) {
        if (useLegacyBindPassword) {
            return Settings.builder()
                .put(getFullSettingKey(realmIdentifier, PoolingSessionFactorySettings.LEGACY_BIND_PASSWORD), password)
                .build();
        } else {
            final MockSecureSettings secureSettings = new MockSecureSettings();
            secureSettings.setString(getFullSettingKey(realmIdentifier, PoolingSessionFactorySettings.SECURE_BIND_PASSWORD), password);
            return Settings.builder().setSecureSettings(secureSettings).build();
        }
    }

}
