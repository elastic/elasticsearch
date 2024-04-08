/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.LDAPURL;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslVerificationMode;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.MockLicenseState;
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
import org.elasticsearch.xpack.core.security.authc.ldap.LdapUserSearchSessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.ldap.SearchGroupsResolverSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapMetadataResolverSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.core.security.authc.support.CachingUsernamePasswordRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.security.authc.support.DnRoleMapperSettings;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.TemplateRoleName;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.Security;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.elasticsearch.xpack.core.security.authc.ldap.LdapSessionFactorySettings.USER_DN_TEMPLATES_SETTING;
import static org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings.URLS_SETTING;
import static org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings.VERIFICATION_MODE_SETTING_REALM;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
    private MockLicenseState licenseState;

    @Before
    public void init() throws Exception {
        threadPool = new TestThreadPool("ldap realm tests");
        resourceWatcherService = new ResourceWatcherService(Settings.EMPTY, threadPool);
        defaultGlobalSettings = Settings.builder().put("path.home", createTempDir()).build();
        sslService = new SSLService(TestEnvironment.newEnvironment(defaultGlobalSettings));
        licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(Security.DELEGATED_AUTHORIZATION_FEATURE)).thenReturn(true);
    }

    @After
    public void shutdown() throws InterruptedException {
        resourceWatcherService.close();
        terminate(threadPool);
    }

    public void testAuthenticateSubTreeGroupSearch() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = Settings.builder()
            .put(defaultGlobalSettings)
            .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
            .put(getFullSettingKey(REALM_IDENTIFIER, RealmSettings.ORDER_SETTING), 0)
            .build();
        RealmConfig config = getRealmConfig(REALM_IDENTIFIER, settings);
        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        LdapRealm ldap = new LdapRealm(config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService), threadPool);
        ldap.initialize(Collections.singleton(ldap), licenseState);

        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        final AuthenticationResult<User> result = future.actionGet();
        assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        User user = result.getValue();
        assertThat(user, notNullValue());
        assertThat(user.roles(), arrayContaining("HMS Victory"));
        assertThat(user.metadata(), notNullValue());
        assertThat(user.metadata().get("ldap_dn"), equalTo("cn=" + VALID_USERNAME + ",ou=people,o=sevenSeas"));
        assertThat(user.metadata().get("ldap_groups"), instanceOf(List.class));
        assertThat(user.metadata().get("mail"), nullValue());
        assertThat(user.metadata().get("cn"), nullValue());
        assertThat((List<?>) user.metadata().get("ldap_groups"), contains("cn=HMS Victory,ou=crews,ou=groups,o=sevenSeas"));
        assertThat(user.email(), equalTo("thardy@royalnavy.mod.uk"));
        assertThat(user.fullName(), equalTo("Thomas Masterman Hardy"));
    }

    public void testAuthenticateMapFullNameAndEmailMetadata() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        boolean misssingSetting = randomBoolean();
        Settings settings = Settings.builder()
            .put(defaultGlobalSettings)
            .put(buildLdapSettings(ldapUrls(), VALID_USER_TEMPLATE, groupSearchBase, LdapSearchScope.SUB_TREE))
            .put(getFullSettingKey(REALM_IDENTIFIER, RealmSettings.ORDER_SETTING), 0)
            .put(
                getFullSettingKey(REALM_IDENTIFIER, LdapMetadataResolverSettings.FULL_NAME_SETTING),
                misssingSetting ? "thisdoesnotexist" : "description"
            )
            .put(getFullSettingKey(REALM_IDENTIFIER, LdapMetadataResolverSettings.EMAIL_SETTING), "uid")
            .build();
        RealmConfig config = getRealmConfig(REALM_IDENTIFIER, settings);
        SessionFactory ldapFactory = LdapRealm.sessionFactory(config, sslService, threadPool);
        LdapRealm ldap = new LdapRealm(config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService), threadPool);
        ldap.initialize(Collections.singleton(ldap), licenseState);

        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken("John Samuel", new SecureString(PASSWORD)), future);
        final AuthenticationResult<User> result = future.actionGet();
        assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        User user = result.getValue();
        assertThat(user, notNullValue());
        assertThat(user.email(), equalTo("jsamuel@royalnavy.mod.uk"));
        assertThat(user.fullName(), equalTo(misssingSetting ? null : "Clerk John Samuel"));
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
            .put(getFullSettingKey(REALM_IDENTIFIER, RealmSettings.ORDER_SETTING), 0)
            .build();
        RealmConfig config = getRealmConfig(REALM_IDENTIFIER, settings);

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        LdapRealm ldap = new LdapRealm(config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService), threadPool);
        ldap.initialize(Collections.singleton(ldap), licenseState);

        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        final AuthenticationResult<User> result = future.actionGet();
        assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        User user = result.getValue();
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
            .put(getFullSettingKey(REALM_IDENTIFIER, RealmSettings.ORDER_SETTING), 0)
            .build();
        RealmConfig config = getRealmConfig(REALM_IDENTIFIER, settings);

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        ldapFactory = spy(ldapFactory);
        LdapRealm ldap = new LdapRealm(config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService), threadPool);
        ldap.initialize(Collections.singleton(ldap), licenseState);

        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        assertThat(future.actionGet().getStatus(), is(AuthenticationResult.Status.SUCCESS));

        future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        assertThat(future.actionGet().getStatus(), is(AuthenticationResult.Status.SUCCESS));

        // verify one and only one session -> caching is working
        verify(ldapFactory, times(1)).session(anyString(), any(SecureString.class), anyActionListener());
    }

    public void testAuthenticateCachingRefresh() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = Settings.builder()
            .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
            .put(defaultGlobalSettings)
            .put(getFullSettingKey(REALM_IDENTIFIER, RealmSettings.ORDER_SETTING), 0)
            .build();
        RealmConfig config = getRealmConfig(REALM_IDENTIFIER, settings);

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        DnRoleMapper roleMapper = buildGroupAsRoleMapper(resourceWatcherService);
        ldapFactory = spy(ldapFactory);
        LdapRealm ldap = new LdapRealm(config, ldapFactory, roleMapper, threadPool);
        ldap.initialize(Collections.singleton(ldap), licenseState);

        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        future.actionGet();
        future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        future.actionGet();

        // verify one and only one session -> caching is working
        verify(ldapFactory, times(1)).session(anyString(), any(SecureString.class), anyActionListener());

        roleMapper.notifyRefresh();

        future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        future.actionGet();

        // we need to session again
        verify(ldapFactory, times(2)).session(anyString(), any(SecureString.class), anyActionListener());
    }

    public void testAuthenticateNoncaching() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = Settings.builder()
            .put(defaultGlobalSettings)
            .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
            .put(getFullSettingKey(REALM_IDENTIFIER, CachingUsernamePasswordRealmSettings.CACHE_TTL_SETTING), -1)
            .put(getFullSettingKey(REALM_IDENTIFIER, RealmSettings.ORDER_SETTING), 0)
            .build();
        RealmConfig config = getRealmConfig(REALM_IDENTIFIER, settings);

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        ldapFactory = spy(ldapFactory);
        LdapRealm ldap = new LdapRealm(config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService), threadPool);
        ldap.initialize(Collections.singleton(ldap), licenseState);

        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        future.actionGet();
        future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        future.actionGet();

        // verify two and only two binds -> caching is disabled
        verify(ldapFactory, times(2)).session(anyString(), any(SecureString.class), anyActionListener());
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
        builder.put(getFullSettingKey(REALM_IDENTIFIER, RealmSettings.ORDER_SETTING), 0);

        final Settings realmSettings = builder.build();
        final Environment env = TestEnvironment.newEnvironment(defaultGlobalSettings);
        RealmConfig config = new RealmConfig(REALM_IDENTIFIER, realmSettings, env, threadPool.getThreadContext());

        final LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        final DnRoleMapper roleMapper = buildGroupAsRoleMapper(resourceWatcherService);
        final LdapRealm ldap = new LdapRealm(config, ldapFactory, roleMapper, threadPool);

        RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("mock", "mock_lookup");
        final MockLookupRealm mockLookup = new MockLookupRealm(
            new RealmConfig(
                realmIdentifier,
                Settings.builder()
                    .put(defaultGlobalSettings)
                    .put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0)
                    .build(),
                env,
                threadPool.getThreadContext()
            )
        );

        ldap.initialize(Arrays.asList(ldap, mockLookup), licenseState);
        mockLookup.initialize(Arrays.asList(ldap, mockLookup), licenseState);

        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        final AuthenticationResult<User> result1 = future.actionGet();
        assertThat(result1.getStatus(), equalTo(AuthenticationResult.Status.CONTINUE));
        assertThat(
            result1.getMessage(),
            equalTo("the principal [" + VALID_USERNAME + "] was authenticated, but no user could be found in realms [mock/mock_lookup]")
        );

        future = new PlainActionFuture<>();
        final User fakeUser = new User(VALID_USERNAME, "fake_role");
        mockLookup.registerUser(fakeUser);
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        final AuthenticationResult<User> result2 = future.actionGet();
        assertThat(result2.getStatus(), equalTo(AuthenticationResult.Status.SUCCESS));
        assertThat(result2.getValue(), sameInstance(fakeUser));
    }

    public void testLdapRealmSelectsLdapSessionFactory() throws Exception {
        final RealmConfig.RealmIdentifier identifier = new RealmConfig.RealmIdentifier(LdapRealmSettings.LDAP_TYPE, "test-ldap-realm");
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = Settings.builder()
            .put(defaultGlobalSettings)

            .putList(getFullSettingKey(identifier, URLS_SETTING), ldapUrls())
            .putList(getFullSettingKey(identifier.getName(), USER_DN_TEMPLATES_SETTING), userTemplate)
            .put(getFullSettingKey(identifier, SearchGroupsResolverSettings.BASE_DN), groupSearchBase)
            .put(getFullSettingKey(identifier, SearchGroupsResolverSettings.SCOPE), LdapSearchScope.SUB_TREE)
            .put(getFullSettingKey(identifier, VERIFICATION_MODE_SETTING_REALM), SslVerificationMode.CERTIFICATE)
            .put(getFullSettingKey(identifier, RealmSettings.ORDER_SETTING), 0)
            .build();
        RealmConfig config = getRealmConfig(identifier, settings);
        final SSLService ssl = new SSLService(config.env());
        SessionFactory sessionFactory = LdapRealm.sessionFactory(config, ssl, threadPool);
        assertThat(sessionFactory, is(instanceOf(LdapSessionFactory.class)));
    }

    public void testLdapRealmSelectsLdapUserSearchSessionFactory() throws Exception {
        final RealmConfig.RealmIdentifier identifier = new RealmConfig.RealmIdentifier(
            LdapRealmSettings.LDAP_TYPE,
            "test-ldap-realm-user-search"
        );
        String groupSearchBase = "o=sevenSeas";
        Settings settings = Settings.builder()
            .put(defaultGlobalSettings)
            .putList(getFullSettingKey(identifier, URLS_SETTING), ldapUrls())
            .put(getFullSettingKey(identifier.getName(), LdapUserSearchSessionFactorySettings.SEARCH_BASE_DN), "")
            .put(getFullSettingKey(identifier, PoolingSessionFactorySettings.BIND_DN), "cn=Thomas Masterman Hardy,ou=people,o=sevenSeas")
            .setSecureSettings(secureSettings(PoolingSessionFactorySettings.SECURE_BIND_PASSWORD, identifier, PASSWORD))
            .put(getFullSettingKey(identifier, SearchGroupsResolverSettings.BASE_DN), groupSearchBase)
            .put(getFullSettingKey(identifier, SearchGroupsResolverSettings.SCOPE), LdapSearchScope.SUB_TREE)
            .put(getFullSettingKey(identifier, VERIFICATION_MODE_SETTING_REALM), SslVerificationMode.CERTIFICATE)
            .put(getFullSettingKey(identifier, RealmSettings.ORDER_SETTING), 0)
            .build();
        final RealmConfig config = getRealmConfig(identifier, settings);
        SessionFactory sessionFactory = LdapRealm.sessionFactory(config, new SSLService(config.env()), threadPool);
        try {
            assertThat(sessionFactory, is(instanceOf(LdapUserSearchSessionFactory.class)));
        } finally {
            ((LdapUserSearchSessionFactory) sessionFactory).close();
        }
    }

    public void testLdapRealmThrowsExceptionForUserTemplateAndSearchSettings() throws Exception {
        final RealmConfig.RealmIdentifier identifier = new RealmConfig.RealmIdentifier(
            LdapRealmSettings.LDAP_TYPE,
            "test-ldap-realm-user-search"
        );

        final List<? extends Setting.AffixSetting<?>> userSearchSettings = List.of(
            LdapUserSearchSessionFactorySettings.SEARCH_BASE_DN,
            LdapUserSearchSessionFactorySettings.SEARCH_ATTRIBUTE,
            LdapUserSearchSessionFactorySettings.SEARCH_SCOPE,
            LdapUserSearchSessionFactorySettings.SEARCH_FILTER,
            LdapUserSearchSessionFactorySettings.POOL_ENABLED
        );
        final List<? extends Setting.AffixSetting<?>> configuredUserSearchSettings = randomNonEmptySubsetOf(userSearchSettings).stream()
            .sorted(Comparator.comparing(userSearchSettings::indexOf))
            .toList();

        final Settings.Builder settingsBuilder = Settings.builder()
            .put(defaultGlobalSettings)
            .putList(getFullSettingKey(identifier, URLS_SETTING), ldapUrls())
            .putList(getFullSettingKey(identifier.getName(), USER_DN_TEMPLATES_SETTING), "cn=foo")
            .put(getFullSettingKey(identifier, SearchGroupsResolverSettings.BASE_DN), "")
            .put(getFullSettingKey(identifier, SearchGroupsResolverSettings.SCOPE), LdapSearchScope.SUB_TREE)
            .put(getFullSettingKey(identifier, VERIFICATION_MODE_SETTING_REALM), SslVerificationMode.CERTIFICATE)
            .put(getFullSettingKey(identifier, RealmSettings.ORDER_SETTING), 0);

        configuredUserSearchSettings.forEach(s -> {
            final String key = getFullSettingKey(identifier.getName(), s);
            settingsBuilder.put(key, key.endsWith(".enabled") ? String.valueOf(randomBoolean()) : randomAlphaOfLengthBetween(8, 18));
        });
        RealmConfig config = getRealmConfig(identifier, settingsBuilder.build());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> LdapRealm.sessionFactory(config, null, threadPool));

        assertThat(
            e.getMessage(),
            containsString(
                "settings were found for both"
                    + " user search ["
                    + configuredUserSearchSettings.stream()
                        .map(Setting::getKey)
                        .map(
                            key -> "xpack.security.authc.realms.ldap.test-ldap-realm-user-search"
                                + key.substring(key.lastIndexOf(".user_search."))
                        )
                        .collect(Collectors.joining(","))
                    + "] and"
                    + " user template [xpack.security.authc.realms.ldap.test-ldap-realm-user-search.user_dn_templates]"
            )
        );
    }

    public void testLdapRealmThrowsExceptionWhenNeitherUserTemplateNorSearchSettingsProvided() throws Exception {
        final RealmConfig.RealmIdentifier identifier = new RealmConfig.RealmIdentifier(
            LdapRealmSettings.LDAP_TYPE,
            "test-ldap-realm-user-search"
        );
        Settings settings = Settings.builder()
            .put(defaultGlobalSettings)
            .putList(getFullSettingKey(identifier, URLS_SETTING), ldapUrls())
            .put(getFullSettingKey(identifier, SearchGroupsResolverSettings.BASE_DN), "")
            .put(getFullSettingKey(identifier, SearchGroupsResolverSettings.SCOPE), LdapSearchScope.SUB_TREE)
            .put(getFullSettingKey(identifier, VERIFICATION_MODE_SETTING_REALM), SslVerificationMode.CERTIFICATE)
            .put(getFullSettingKey(identifier, RealmSettings.ORDER_SETTING), 0)
            .build();
        RealmConfig config = getRealmConfig(identifier, settings);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> LdapRealm.sessionFactory(config, null, threadPool));
        assertThat(
            e.getMessage(),
            containsString(
                "settings were not found for either"
                    + " user search [xpack.security.authc.realms.ldap.test-ldap-realm-user-search.user_search.base_dn] or"
                    + " user template [xpack.security.authc.realms.ldap.test-ldap-realm-user-search.user_dn_templates]"
            )
        );
    }

    public void testLdapRealmMapsUserDNToRole() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = Settings.builder()
            .put(defaultGlobalSettings)
            .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
            .put(
                getFullSettingKey(REALM_IDENTIFIER, DnRoleMapperSettings.ROLE_MAPPING_FILE_SETTING),
                getDataPath("/org/elasticsearch/xpack/security/authc/support/role_mapping.yml")
            )
            .build();
        RealmConfig config = getRealmConfig(REALM_IDENTIFIER, settings);

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        LdapRealm ldap = new LdapRealm(config, ldapFactory, new DnRoleMapper(config, resourceWatcherService), threadPool);
        ldap.initialize(Collections.singleton(ldap), licenseState);

        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken("Horatio Hornblower", new SecureString(PASSWORD)), future);
        final AuthenticationResult<User> result = future.actionGet();
        assertThat(result, notNullValue());
        assertThat(result.toString(), result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        User user = result.getValue();
        assertThat(user, notNullValue());
        assertThat(user.toString(), user.roles(), arrayContaining("avenger"));
    }

    /**
     * This tests template role mappings (see
     * {@link TemplateRoleName}) with an LDAP realm, using a additional
     * metadata field (see {@link LdapMetadataResolverSettings#ADDITIONAL_METADATA_SETTING}).
     */
    public void testLdapRealmWithTemplatedRoleMapping() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = Settings.builder()
            .put(defaultGlobalSettings)
            .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
            .put(
                getFullSettingKey(
                    REALM_IDENTIFIER.getName(),
                    LdapMetadataResolverSettings.ADDITIONAL_METADATA_SETTING.apply(LdapRealmSettings.LDAP_TYPE)
                ),
                "uid"
            )
            .build();
        RealmConfig config = getRealmConfig(REALM_IDENTIFIER, settings);

        SecurityIndexManager mockSecurityIndex = mock(SecurityIndexManager.class);
        when(mockSecurityIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(true);
        when(mockSecurityIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(true);
        when(mockSecurityIndex.isIndexUpToDate()).thenReturn(true);
        when(mockSecurityIndex.defensiveCopy()).thenReturn(mockSecurityIndex);
        when(mockSecurityIndex.indexExists()).thenReturn(true);

        Client mockClient = mock(Client.class);
        when(mockClient.threadPool()).thenReturn(threadPool);

        final ScriptService scriptService = new ScriptService(
            defaultGlobalSettings,
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine()),
            ScriptModule.CORE_CONTEXTS,
            () -> 1L
        );
        NativeRoleMappingStore roleMapper = new NativeRoleMappingStore(
            defaultGlobalSettings,
            mockClient,
            mockSecurityIndex,
            scriptService
        ) {
            @Override
            protected void loadMappings(ActionListener<List<ExpressionRoleMapping>> listener) {
                listener.onResponse(Arrays.asList(NativeRoleMappingStore.buildMapping("m1", new BytesArray("""
                    {
                        "role_templates": [ { "template": { "source": "_user_{{metadata.uid}}" } } ],
                        "enabled": true,
                        "rules": {
                          "any": [ { "field": { "realm.name": "ldap1" } }, { "field": { "realm.name": "ldap2" } } ]
                        }
                      }""")), NativeRoleMappingStore.buildMapping("m2", new BytesArray("""
                    {
                      "roles": [ "should_not_happen" ],
                      "enabled": true,
                      "rules": {
                        "all": [ { "field": { "realm.name": "ldap1" } }, { "field": { "realm.name": "ldap2" } } ]
                      }
                    }""")), NativeRoleMappingStore.buildMapping("m3", new BytesArray("""
                    {
                      "roles": [ "sales_admin" ],
                      "enabled": true,
                      "rules": {
                        "field": {
                          "dn": "*,ou=people,o=sevenSeas"
                        }
                      }
                    }"""))));
            }
        };
        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        LdapRealm ldap = new LdapRealm(config, ldapFactory, roleMapper, threadPool);
        ldap.initialize(Collections.singleton(ldap), licenseState);

        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken("Horatio Hornblower", new SecureString(PASSWORD)), future);
        final AuthenticationResult<User> result = future.actionGet();
        assertThat(result, notNullValue());
        assertThat(result.toString(), result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        User user = result.getValue();
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
            .put(buildLdapSettings(new String[] { url.toString() }, userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
            .put(getFullSettingKey(REALM_IDENTIFIER, RealmSettings.ORDER_SETTING), 0)
            .build();
        RealmConfig config = getRealmConfig(REALM_IDENTIFIER, settings);
        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        LdapRealm ldap = new LdapRealm(config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService), threadPool);
        ldap.initialize(Collections.singleton(ldap), licenseState);

        PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        final AuthenticationResult<User> result = future.actionGet();
        assertThat(result, notNullValue());
        assertThat(result.toString(), result.getStatus(), is(AuthenticationResult.Status.CONTINUE));
        assertThat(result.getValue(), nullValue());
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
            .put(getFullSettingKey(identifier, PoolingSessionFactorySettings.BIND_DN), "cn=Thomas Masterman Hardy,ou=people,o=sevenSeas")
            .put(getFullSettingKey(identifier, PoolingSessionFactorySettings.LEGACY_BIND_PASSWORD), PASSWORD)
            .put(getFullSettingKey(identifier, SearchGroupsResolverSettings.BASE_DN), groupSearchBase)
            .put(getFullSettingKey(identifier, SearchGroupsResolverSettings.SCOPE), LdapSearchScope.SUB_TREE)
            .put(getFullSettingKey(identifier.getName(), USER_DN_TEMPLATES_SETTING), "--")
            .put(getFullSettingKey(identifier, VERIFICATION_MODE_SETTING_REALM), SslVerificationMode.CERTIFICATE);

        int order = randomIntBetween(0, 10);
        settings.put(getFullSettingKey(identifier, RealmSettings.ORDER_SETTING), order);

        boolean userSearch = randomBoolean();
        if (userSearch) {
            settings.put(getFullSettingKey(identifier.getName(), LdapUserSearchSessionFactorySettings.SEARCH_BASE_DN), "");
        }

        RealmConfig config = getRealmConfig(identifier, settings.build());

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, new SSLService(config.env()), threadPool);
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

    private SecureSettings secureSettings(
        Function<String, Setting.AffixSetting<SecureString>> settingFactory,
        RealmConfig.RealmIdentifier identifier,
        String value
    ) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(getFullSettingKey(identifier, settingFactory), value);
        return secureSettings;
    }
}
