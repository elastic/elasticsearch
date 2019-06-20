/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.LDAPURL;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapSessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapUserSearchSessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.ldap.SearchGroupsResolverSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapMetaDataResolverSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.core.security.authc.support.CachingUsernamePasswordRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.security.authc.support.DnRoleMapperSettings;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.TemplateRoleName;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.VerificationMode;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapTestCase;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.xpack.security.authc.support.DnRoleMapper;
import org.elasticsearch.xpack.security.authc.support.MockLookupRealm;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings.URLS_SETTING;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LdapRealmTests extends LdapTestCase {

    public static final String VALID_USER_TEMPLATE = "cn={0},ou=people,o=sevenSeas";
    public static final String VALID_USERNAME = "Thomas Masterman Hardy";
    public static final String PASSWORD = "pass";

    private ThreadPool threadPool;
    private ResourceWatcherService resourceWatcherService;
    private Settings defaultGlobalSettings;
    private SSLService sslService;
    private XPackLicenseState licenseState;

    @Before
    public void init() throws Exception {
        threadPool = new TestThreadPool("ldap realm tests");
        resourceWatcherService = new ResourceWatcherService(Settings.EMPTY, threadPool);
        defaultGlobalSettings = Settings.builder().put("path.home", createTempDir()).build();
        sslService = new SSLService(defaultGlobalSettings, TestEnvironment.newEnvironment(defaultGlobalSettings));
        licenseState = mock(XPackLicenseState.class);
        when(licenseState.isAuthorizationRealmAllowed()).thenReturn(true);
    }

    @After
    public void shutdown() throws InterruptedException {
        resourceWatcherService.stop();
        terminate(threadPool);
    }

    public void testAuthenticateSubTreeGroupSearch() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = Settings.builder()
            .put(defaultGlobalSettings)
            .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
            .build();
        RealmConfig config = getRealmConfig(REALM_IDENTIFIER, settings);
        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        LdapRealm ldap = new LdapRealm(config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService),
                threadPool);
        ldap.initialize(Collections.singleton(ldap), licenseState);

        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        final AuthenticationResult result = future.actionGet();
        assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        User user = result.getUser();
        assertThat(user, notNullValue());
        assertThat(user.roles(), arrayContaining("HMS Victory"));
        assertThat(user.metadata(), notNullValue());
        assertThat(user.metadata().get("ldap_dn"), equalTo("cn=" + VALID_USERNAME + ",ou=people,o=sevenSeas"));
        assertThat(user.metadata().get("ldap_groups"), instanceOf(List.class));
        assertThat((List<?>) user.metadata().get("ldap_groups"), contains("cn=HMS Victory,ou=crews,ou=groups,o=sevenSeas"));
    }

    private RealmConfig getRealmConfig(RealmConfig.RealmIdentifier identifier, Settings settings) {
        final Environment env = TestEnvironment.newEnvironment(settings);
        return new RealmConfig(identifier, settings, env, new ThreadContext(settings));
    }

    public void testAuthenticateOneLevelGroupSearch() throws Exception {
        String groupSearchBase = "ou=crews,ou=groups,o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = Settings.builder()
                .put(defaultGlobalSettings)
                .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.ONE_LEVEL))
                .build();
        RealmConfig config = getRealmConfig(REALM_IDENTIFIER, settings);

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        LdapRealm ldap =
                new LdapRealm(config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService), threadPool);
        ldap.initialize(Collections.singleton(ldap), licenseState);

        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        final AuthenticationResult result = future.actionGet();
        assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        User user = result.getUser();
        assertThat(user, notNullValue());
        assertThat("For roles " + Arrays.toString(user.roles()), user.roles(), arrayContaining("HMS Victory"));
        assertThat(user.metadata(), notNullValue());
        assertThat(user.metadata().get("ldap_dn"), equalTo("cn=" + VALID_USERNAME + ",ou=people,o=sevenSeas"));
        assertThat(user.metadata().get("ldap_groups"), instanceOf(List.class));
        assertThat((List<?>) user.metadata().get("ldap_groups"), contains("cn=HMS Victory,ou=crews,ou=groups,o=sevenSeas"));
    }

    public void testAuthenticateCaching() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = Settings.builder()
                .put(defaultGlobalSettings)
                .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
                .build();
        RealmConfig config = getRealmConfig(REALM_IDENTIFIER, settings);

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        ldapFactory = spy(ldapFactory);
        LdapRealm ldap =
                new LdapRealm(config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService), threadPool);
        ldap.initialize(Collections.singleton(ldap), licenseState);

        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        assertThat(future.actionGet().getStatus(), is(AuthenticationResult.Status.SUCCESS));

        future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        assertThat(future.actionGet().getStatus(), is(AuthenticationResult.Status.SUCCESS));

        //verify one and only one session -> caching is working
        verify(ldapFactory, times(1)).session(anyString(), any(SecureString.class), any(ActionListener.class));
    }

    public void testAuthenticateCachingRefresh() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = Settings.builder()
                .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put(defaultGlobalSettings)
                .build();
        RealmConfig config = getRealmConfig(REALM_IDENTIFIER, settings);

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        DnRoleMapper roleMapper = buildGroupAsRoleMapper(resourceWatcherService);
        ldapFactory = spy(ldapFactory);
        LdapRealm ldap = new LdapRealm(config, ldapFactory, roleMapper, threadPool);
        ldap.initialize(Collections.singleton(ldap), licenseState);

        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        future.actionGet();
        future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        future.actionGet();

        //verify one and only one session -> caching is working
        verify(ldapFactory, times(1)).session(anyString(), any(SecureString.class), any(ActionListener.class));

        roleMapper.notifyRefresh();

        future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        future.actionGet();

        //we need to session again
        verify(ldapFactory, times(2)).session(anyString(), any(SecureString.class), any(ActionListener.class));
    }

    public void testAuthenticateNoncaching() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = Settings.builder()
                .put(defaultGlobalSettings)
                .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put(getFullSettingKey(REALM_IDENTIFIER, CachingUsernamePasswordRealmSettings.CACHE_TTL_SETTING), -1)
                .build();
        RealmConfig config = getRealmConfig(REALM_IDENTIFIER, settings);

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        ldapFactory = spy(ldapFactory);
        LdapRealm ldap =
                new LdapRealm(config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService), threadPool);
        ldap.initialize(Collections.singleton(ldap), licenseState);

        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        future.actionGet();
        future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        future.actionGet();

        //verify two and only two binds -> caching is disabled
        verify(ldapFactory, times(2)).session(anyString(), any(SecureString.class), any(ActionListener.class));
    }

    public void testDelegatedAuthorization() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        final Settings.Builder builder = Settings.builder()
            .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
            .putList(getFullSettingKey(REALM_IDENTIFIER, DelegatedAuthorizationSettings.AUTHZ_REALMS), "mock_lookup");

        if (randomBoolean()) {
            // maybe disable caching
            builder.put(getFullSettingKey(REALM_IDENTIFIER, CachingUsernamePasswordRealmSettings.CACHE_TTL_SETTING), -1);
        }

        final Settings realmSettings = builder.build();
        final Environment env = TestEnvironment.newEnvironment(defaultGlobalSettings);
        RealmConfig config = new RealmConfig(REALM_IDENTIFIER, realmSettings, env, threadPool.getThreadContext());

        final LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        final DnRoleMapper roleMapper = buildGroupAsRoleMapper(resourceWatcherService);
        final LdapRealm ldap = new LdapRealm(config, ldapFactory, roleMapper, threadPool);

        final MockLookupRealm mockLookup = new MockLookupRealm(new RealmConfig(new RealmConfig.RealmIdentifier("mock", "mock_lookup"),
            defaultGlobalSettings, env, threadPool.getThreadContext()));

        ldap.initialize(Arrays.asList(ldap, mockLookup), licenseState);
        mockLookup.initialize(Arrays.asList(ldap, mockLookup), licenseState);

        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        final AuthenticationResult result1 = future.actionGet();
        assertThat(result1.getStatus(), equalTo(AuthenticationResult.Status.CONTINUE));
        assertThat(result1.getMessage(),
            equalTo("the principal [" + VALID_USERNAME + "] was authenticated, but no user could be found in realms [mock/mock_lookup]"));

        future = new PlainActionFuture<>();
        final User fakeUser = new User(VALID_USERNAME, "fake_role");
        mockLookup.registerUser(fakeUser);
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        final AuthenticationResult result2 = future.actionGet();
        assertThat(result2.getStatus(), equalTo(AuthenticationResult.Status.SUCCESS));
        assertThat(result2.getUser(), sameInstance(fakeUser));
    }

    public void testLdapRealmSelectsLdapSessionFactory() throws Exception {
        final RealmConfig.RealmIdentifier identifier = new RealmConfig.RealmIdentifier(LdapRealmSettings.LDAP_TYPE, "test-ldap-realm");
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = Settings.builder()
                .put(defaultGlobalSettings)
                .putList(getFullSettingKey(identifier, URLS_SETTING), ldapUrls())
                .putList(getFullSettingKey(identifier.getName(), LdapSessionFactorySettings.USER_DN_TEMPLATES_SETTING), userTemplate)
                .put(getFullSettingKey(identifier, SearchGroupsResolverSettings.BASE_DN), groupSearchBase)
                .put(getFullSettingKey(identifier, SearchGroupsResolverSettings.SCOPE), LdapSearchScope.SUB_TREE)
                .put(getFullSettingKey(identifier, SSLConfigurationSettings.VERIFICATION_MODE_SETTING_REALM), VerificationMode.CERTIFICATE)
                .build();
        RealmConfig config = getRealmConfig(identifier, settings);
        SessionFactory sessionFactory = LdapRealm.sessionFactory(config, new SSLService(settings, config.env()), threadPool);
        assertThat(sessionFactory, is(instanceOf(LdapSessionFactory.class)));
    }

    public void testLdapRealmSelectsLdapUserSearchSessionFactory() throws Exception {
        final RealmConfig.RealmIdentifier identifier
                = new RealmConfig.RealmIdentifier(LdapRealmSettings.LDAP_TYPE, "test-ldap-realm-user-search");
        String groupSearchBase = "o=sevenSeas";
        Settings settings = Settings.builder()
                .put(defaultGlobalSettings)
                .putList(getFullSettingKey(identifier, URLS_SETTING), ldapUrls())
                .put(getFullSettingKey(identifier.getName(), LdapUserSearchSessionFactorySettings.SEARCH_BASE_DN), "")
                .put(getFullSettingKey(identifier, PoolingSessionFactorySettings.BIND_DN),
                    "cn=Thomas Masterman Hardy,ou=people,o=sevenSeas")
                .setSecureSettings(secureSettings(PoolingSessionFactorySettings.SECURE_BIND_PASSWORD, identifier, PASSWORD))
                .put(getFullSettingKey(identifier, SearchGroupsResolverSettings.BASE_DN), groupSearchBase)
                .put(getFullSettingKey(identifier, SearchGroupsResolverSettings.SCOPE), LdapSearchScope.SUB_TREE)
                .put(getFullSettingKey(identifier, SSLConfigurationSettings.VERIFICATION_MODE_SETTING_REALM), VerificationMode.CERTIFICATE)
                .build();
        final RealmConfig config = getRealmConfig(identifier, settings);
        SessionFactory sessionFactory = LdapRealm.sessionFactory(config, new SSLService(config.settings(), config.env()), threadPool);
        try {
            assertThat(sessionFactory, is(instanceOf(LdapUserSearchSessionFactory.class)));
        } finally {
            ((LdapUserSearchSessionFactory) sessionFactory).close();
        }
    }

    public void testLdapRealmThrowsExceptionForUserTemplateAndSearchSettings() throws Exception {
        final RealmConfig.RealmIdentifier identifier
                = new RealmConfig.RealmIdentifier(LdapRealmSettings.LDAP_TYPE, "test-ldap-realm-user-search");
        Settings settings = Settings.builder()
                .put(defaultGlobalSettings)
                .putList(getFullSettingKey(identifier, URLS_SETTING), ldapUrls())
                .putList(getFullSettingKey(identifier.getName(), LdapSessionFactorySettings.USER_DN_TEMPLATES_SETTING), "cn=foo")
                .put(getFullSettingKey(identifier.getName(), LdapUserSearchSessionFactorySettings.SEARCH_BASE_DN), "cn=bar")
                .put(getFullSettingKey(identifier, SearchGroupsResolverSettings.BASE_DN), "")
                .put(getFullSettingKey(identifier, SearchGroupsResolverSettings.SCOPE), LdapSearchScope.SUB_TREE)
                .put(getFullSettingKey(identifier, SSLConfigurationSettings.VERIFICATION_MODE_SETTING_REALM), VerificationMode.CERTIFICATE)
                .build();
        RealmConfig config = getRealmConfig(identifier, settings);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> LdapRealm.sessionFactory(config, null, threadPool));
        assertThat(e.getMessage(),
                containsString("settings were found for both" +
                        " user search [xpack.security.authc.realms.ldap.test-ldap-realm-user-search.user_search.base_dn] and" +
                        " user template [xpack.security.authc.realms.ldap.test-ldap-realm-user-search.user_dn_templates]"));
    }

    public void testLdapRealmThrowsExceptionWhenNeitherUserTemplateNorSearchSettingsProvided() throws Exception {
        final RealmConfig.RealmIdentifier identifier
                = new RealmConfig.RealmIdentifier(LdapRealmSettings.LDAP_TYPE, "test-ldap-realm-user-search");
        Settings settings = Settings.builder()
                .put(defaultGlobalSettings)
                .putList(getFullSettingKey(identifier, URLS_SETTING), ldapUrls())
                .put(getFullSettingKey(identifier, SearchGroupsResolverSettings.BASE_DN), "")
                .put(getFullSettingKey(identifier, SearchGroupsResolverSettings.SCOPE), LdapSearchScope.SUB_TREE)
                .put(getFullSettingKey(identifier, SSLConfigurationSettings.VERIFICATION_MODE_SETTING_REALM), VerificationMode.CERTIFICATE)
                .build();
        RealmConfig config = getRealmConfig(identifier, settings);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> LdapRealm.sessionFactory(config, null, threadPool));
        assertThat(e.getMessage(),
                containsString("settings were not found for either" +
                        " user search [xpack.security.authc.realms.ldap.test-ldap-realm-user-search.user_search.base_dn] or" +
                        " user template [xpack.security.authc.realms.ldap.test-ldap-realm-user-search.user_dn_templates]"));
    }

    public void testLdapRealmMapsUserDNToRole() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = Settings.builder()
                .put(defaultGlobalSettings)
                .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put(getFullSettingKey(REALM_IDENTIFIER, DnRoleMapperSettings.ROLE_MAPPING_FILE_SETTING),
                        getDataPath("/org/elasticsearch/xpack/security/authc/support/role_mapping.yml"))
                .build();
        RealmConfig config = getRealmConfig(REALM_IDENTIFIER, settings);

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        LdapRealm ldap = new LdapRealm(config, ldapFactory,
                new DnRoleMapper(config, resourceWatcherService), threadPool);
        ldap.initialize(Collections.singleton(ldap), licenseState);

        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken("Horatio Hornblower", new SecureString(PASSWORD)), future);
        final AuthenticationResult result = future.actionGet();
        assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        User user = result.getUser();
        assertThat(user, notNullValue());
        assertThat(user.roles(), arrayContaining("avenger"));
    }

    /**
     * This tests template role mappings (see
     * {@link TemplateRoleName}) with an LDAP realm, using a additional
     * metadata field (see {@link LdapMetaDataResolverSettings#ADDITIONAL_META_DATA_SETTING}).
     */
    public void testLdapRealmWithTemplatedRoleMapping() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = Settings.builder()
                .put(defaultGlobalSettings)
                .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put(getFullSettingKey(REALM_IDENTIFIER.getName(), LdapMetaDataResolverSettings.ADDITIONAL_META_DATA_SETTING), "uid")
                .build();
        RealmConfig config = getRealmConfig(REALM_IDENTIFIER, settings);

        SecurityIndexManager mockSecurityIndex = mock(SecurityIndexManager.class);
        when(mockSecurityIndex.isAvailable()).thenReturn(true);
        when(mockSecurityIndex.isIndexUpToDate()).thenReturn(true);
        when(mockSecurityIndex.isMappingUpToDate()).thenReturn(true);

        Client mockClient = mock(Client.class);
        when(mockClient.threadPool()).thenReturn(threadPool);

        final ScriptService scriptService = new ScriptService(defaultGlobalSettings,
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine()), ScriptModule.CORE_CONTEXTS);
        NativeRoleMappingStore roleMapper = new NativeRoleMappingStore(defaultGlobalSettings, mockClient, mockSecurityIndex,
            scriptService) {
            @Override
            protected void loadMappings(ActionListener<List<ExpressionRoleMapping>> listener) {
                listener.onResponse(
                    Arrays.asList(
                        this.buildMapping("m1", new BytesArray("{" +
                            "\"role_templates\":[{\"template\":{\"source\":\"_user_{{metadata.uid}}\"}}]," +
                            "\"enabled\":true," +
                            "\"rules\":{ \"any\":[" +
                            " { \"field\":{\"realm.name\":\"ldap1\"}}," +
                            " { \"field\":{\"realm.name\":\"ldap2\"}}" +
                            "]}}")),
                        this.buildMapping("m2", new BytesArray("{" +
                            "\"roles\":[\"should_not_happen\"]," +
                            "\"enabled\":true," +
                            "\"rules\":{ \"all\":[" +
                            " { \"field\":{\"realm.name\":\"ldap1\"}}," +
                            " { \"field\":{\"realm.name\":\"ldap2\"}}" +
                            "]}}")),
                        this.buildMapping("m3", new BytesArray("{" +
                            "\"roles\":[\"sales_admin\"]," +
                            "\"enabled\":true," +
                            "\"rules\":" +
                            " { \"field\":{\"dn\":\"*,ou=people,o=sevenSeas\"}}" +
                            "}"))
                    )
                );
            }
        };
        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        LdapRealm ldap = new LdapRealm(config, ldapFactory,
            roleMapper, threadPool);
        ldap.initialize(Collections.singleton(ldap), licenseState);

        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken("Horatio Hornblower", new SecureString(PASSWORD)), future);
        final AuthenticationResult result = future.actionGet();
        assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        User user = result.getUser();
        assertThat(user, notNullValue());
        assertThat(user.roles(), arrayContainingInAnyOrder("_user_hhornblo", "sales_admin"));
    }

    /**
     * The contract for {@link Realm} implementations is that they should log-and-return-null (and
     * not call {@link ActionListener#onFailure(Exception)}) if there is an internal exception that prevented them from performing an
     * authentication.
     * This method tests that when an LDAP server is unavailable (invalid hostname), there is a <code>null</code> result
     * rather than an exception.
     */
    public void testLdapConnectionFailureIsTreatedAsAuthenticationFailure() throws Exception {
        LDAPURL url = new LDAPURL("ldap", "..", 12345, null, null, null, null);
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = Settings.builder()
            .put(defaultGlobalSettings)
            .put(buildLdapSettings(new String[]{url.toString()}, userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
            .build();
        RealmConfig config = getRealmConfig(REALM_IDENTIFIER, settings);
        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        LdapRealm ldap = new LdapRealm(config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService),
                threadPool);
        ldap.initialize(Collections.singleton(ldap), licenseState);

        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        final AuthenticationResult result = future.actionGet();
        assertThat(result.getStatus(), is(AuthenticationResult.Status.CONTINUE));
        assertThat(result.getUser(), nullValue());
        assertThat(result.getMessage(), is("authenticate failed"));
        assertThat(result.getException(), notNullValue());
        assertThat(result.getException().getMessage(), containsString("UnknownHostException"));
    }

    public void testUsageStats() throws Exception {
        final RealmConfig.RealmIdentifier identifier = new RealmConfig.RealmIdentifier(LdapRealmSettings.LDAP_TYPE, "ldap-realm");
        String groupSearchBase = "o=sevenSeas";
        Settings.Builder settings = Settings.builder()
                .put(defaultGlobalSettings)
                .putList(getFullSettingKey(identifier, URLS_SETTING), ldapUrls())
                .put(getFullSettingKey(identifier, PoolingSessionFactorySettings.BIND_DN),
                    "cn=Thomas Masterman Hardy,ou=people,o=sevenSeas")
                .put(getFullSettingKey(identifier, PoolingSessionFactorySettings.LEGACY_BIND_PASSWORD), PASSWORD)
                .put(getFullSettingKey(identifier, SearchGroupsResolverSettings.BASE_DN), groupSearchBase)
                .put(getFullSettingKey(identifier, SearchGroupsResolverSettings.SCOPE), LdapSearchScope.SUB_TREE)
                .put(getFullSettingKey(identifier.getName(), LdapSessionFactorySettings.USER_DN_TEMPLATES_SETTING), "--")
                .put(getFullSettingKey(identifier, SSLConfigurationSettings.VERIFICATION_MODE_SETTING_REALM), VerificationMode.CERTIFICATE);

        int order = randomIntBetween(0, 10);
        settings.put(getFullSettingKey(identifier, RealmSettings.ORDER_SETTING), order);

        boolean userSearch = randomBoolean();
        if (userSearch) {
            settings.put(getFullSettingKey(identifier.getName(), LdapUserSearchSessionFactorySettings.SEARCH_BASE_DN), "");
        }

        RealmConfig config = getRealmConfig(identifier, settings.build());

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, new SSLService(config.settings(), config.env()), threadPool);
        LdapRealm realm = new LdapRealm(config, ldapFactory, new DnRoleMapper(config, resourceWatcherService), threadPool);
        realm.initialize(Collections.singleton(realm), licenseState);

        PlainActionFuture<Map<String, Object>> future = new PlainActionFuture<>();
        realm.usageStats(future);
        Map<String, Object> stats = future.get();
        assertThat(stats, is(notNullValue()));
        assertThat(stats, hasEntry("name", identifier.getName()));
        assertThat(stats, hasEntry("order", realm.order()));
        assertThat(stats, hasEntry("size", 0));
        assertThat(stats, hasEntry("ssl", false));
        assertThat(stats, hasEntry("user_search", userSearch));
    }

    private SecureSettings secureSettings(Function<String, Setting.AffixSetting<SecureString>> settingFactory,
                                          RealmConfig.RealmIdentifier identifier, String value) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(getFullSettingKey(identifier, settingFactory), value);
        return secureSettings;
    }
}
