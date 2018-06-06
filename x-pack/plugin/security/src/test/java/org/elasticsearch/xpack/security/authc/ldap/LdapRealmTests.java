/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.LDAPURL;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapSessionFactorySettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.core.security.authc.support.CachingUsernamePasswordRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.DnRoleMapperSettings;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.VerificationMode;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapTestCase;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.xpack.security.authc.support.DnRoleMapper;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.ldap.support.SessionFactorySettings.URLS_SETTING;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class LdapRealmTests extends LdapTestCase {

    public static final String VALID_USER_TEMPLATE = "cn={0},ou=people,o=sevenSeas";
    public static final String VALID_USERNAME = "Thomas Masterman Hardy";
    public static final String PASSWORD = "pass";

    private static final String USER_DN_TEMPLATES_SETTING_KEY = LdapSessionFactorySettings.USER_DN_TEMPLATES_SETTING.getKey();

    private ThreadPool threadPool;
    private ResourceWatcherService resourceWatcherService;
    private Settings globalSettings;
    private SSLService sslService;

    @Before
    public void init() throws Exception {
        threadPool = new TestThreadPool("ldap realm tests");
        resourceWatcherService = new ResourceWatcherService(Settings.EMPTY, threadPool);
        globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        sslService = new SSLService(globalSettings, TestEnvironment.newEnvironment(globalSettings));
    }

    @After
    public void shutdown() throws InterruptedException {
        resourceWatcherService.stop();
        terminate(threadPool);
    }

    public void testAuthenticateSubTreeGroupSearch() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE);
        RealmConfig config = new RealmConfig("test-ldap-realm", settings, globalSettings, TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));
        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        LdapRealm ldap = new LdapRealm(LdapRealmSettings.LDAP_TYPE, config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService),
                threadPool);

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

    public void testAuthenticateOneLevelGroupSearch() throws Exception {
        String groupSearchBase = "ou=crews,ou=groups,o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = Settings.builder()
                .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.ONE_LEVEL))
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm", settings, globalSettings, TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        LdapRealm ldap =
                new LdapRealm(LdapRealmSettings.LDAP_TYPE, config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService), threadPool);

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
                .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm", settings, globalSettings, TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        ldapFactory = spy(ldapFactory);
        LdapRealm ldap =
                new LdapRealm(LdapRealmSettings.LDAP_TYPE, config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService), threadPool);

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
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm", settings, globalSettings, TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        DnRoleMapper roleMapper = buildGroupAsRoleMapper(resourceWatcherService);
        ldapFactory = spy(ldapFactory);
        LdapRealm ldap = new LdapRealm(LdapRealmSettings.LDAP_TYPE, config, ldapFactory, roleMapper, threadPool);
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
                .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put(CachingUsernamePasswordRealmSettings.CACHE_TTL_SETTING.getKey(), -1)
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm", settings, globalSettings, TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        ldapFactory = spy(ldapFactory);
        LdapRealm ldap =
                new LdapRealm(LdapRealmSettings.LDAP_TYPE, config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService), threadPool);
        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        future.actionGet();
        future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, new SecureString(PASSWORD)), future);
        future.actionGet();

        //verify two and only two binds -> caching is disabled
        verify(ldapFactory, times(2)).session(anyString(), any(SecureString.class), any(ActionListener.class));
    }

    public void testLdapRealmSelectsLdapSessionFactory() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = Settings.builder()
                .putList(URLS_SETTING, ldapUrls())
                .putList(USER_DN_TEMPLATES_SETTING_KEY, userTemplate)
                .put("group_search.base_dn", groupSearchBase)
                .put("group_search.scope", LdapSearchScope.SUB_TREE)
                .put("ssl.verification_mode", VerificationMode.CERTIFICATE)
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm", settings, globalSettings, TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));
        SessionFactory sessionFactory = LdapRealm.sessionFactory(config, sslService, threadPool, LdapRealmSettings.LDAP_TYPE);
        assertThat(sessionFactory, is(instanceOf(LdapSessionFactory.class)));
    }

    public void testLdapRealmSelectsLdapUserSearchSessionFactory() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        Settings settings = Settings.builder()
                .putList(URLS_SETTING, ldapUrls())
                .put("user_search.base_dn", "")
                .put("bind_dn", "cn=Thomas Masterman Hardy,ou=people,o=sevenSeas")
                .setSecureSettings(secureSettings("secure_bind_password", PASSWORD))
                .put("group_search.base_dn", groupSearchBase)
                .put("group_search.scope", LdapSearchScope.SUB_TREE)
                .put("ssl.verification_mode", VerificationMode.CERTIFICATE)
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm-user-search", settings, globalSettings, TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));
        SessionFactory sessionFactory = LdapRealm.sessionFactory(config, sslService, threadPool, LdapRealmSettings.LDAP_TYPE);
        try {
            assertThat(sessionFactory, is(instanceOf(LdapUserSearchSessionFactory.class)));
        } finally {
            ((LdapUserSearchSessionFactory)sessionFactory).close();
        }
    }

    private MockSecureSettings secureSettings(String key, String value) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(key, value);
        return secureSettings;
    }

    public void testLdapRealmThrowsExceptionForUserTemplateAndSearchSettings() throws Exception {
        Settings settings = Settings.builder()
                .putList(URLS_SETTING, ldapUrls())
                .putList(USER_DN_TEMPLATES_SETTING_KEY, "cn=foo")
                .put("user_search.base_dn", "cn=bar")
                .put("group_search.base_dn", "")
                .put("group_search.scope", LdapSearchScope.SUB_TREE)
                .put("ssl.verification_mode", VerificationMode.CERTIFICATE)
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm-user-search", settings, globalSettings, TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> LdapRealm.sessionFactory(config, null, threadPool, LdapRealmSettings.LDAP_TYPE));
        assertThat(e.getMessage(),
                containsString("settings were found for both" +
                        " user search [xpack.security.authc.realms.test-ldap-realm-user-search.user_search.] and" +
                        " user template [xpack.security.authc.realms.test-ldap-realm-user-search.user_dn_templates]"));
    }

    public void testLdapRealmThrowsExceptionWhenNeitherUserTemplateNorSearchSettingsProvided() throws Exception {
        Settings settings = Settings.builder()
                .putList(URLS_SETTING, ldapUrls())
                .put("group_search.base_dn", "")
                .put("group_search.scope", LdapSearchScope.SUB_TREE)
                .put("ssl.verification_mode", VerificationMode.CERTIFICATE)
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm-user-search", settings, globalSettings, TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> LdapRealm.sessionFactory(config, null, threadPool, LdapRealmSettings.LDAP_TYPE));
        assertThat(e.getMessage(),
                containsString("settings were not found for either" +
                        " user search [xpack.security.authc.realms.test-ldap-realm-user-search.user_search.] or" +
                        " user template [xpack.security.authc.realms.test-ldap-realm-user-search.user_dn_templates]"));
    }

    public void testLdapRealmMapsUserDNToRole() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = Settings.builder()
                .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put(DnRoleMapperSettings.ROLE_MAPPING_FILE_SETTING.getKey(),
                        getDataPath("/org/elasticsearch/xpack/security/authc/support/role_mapping.yml"))
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm-userdn", settings, globalSettings, TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        LdapRealm ldap = new LdapRealm(LdapRealmSettings.LDAP_TYPE, config, ldapFactory,
                new DnRoleMapper(config, resourceWatcherService), threadPool);

        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken("Horatio Hornblower", new SecureString(PASSWORD)), future);
        final AuthenticationResult result = future.actionGet();
        assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        User user = result.getUser();
        assertThat(user, notNullValue());
        assertThat(user.roles(), arrayContaining("avenger"));
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
        Settings settings = buildLdapSettings(new String[] { url.toString() }, userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE);
        RealmConfig config = new RealmConfig("test-ldap-realm", settings, globalSettings, TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));
        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        LdapRealm ldap = new LdapRealm(LdapRealmSettings.LDAP_TYPE, config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService),
                threadPool);

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
        String groupSearchBase = "o=sevenSeas";
        Settings.Builder settings = Settings.builder()
                .putList(URLS_SETTING, ldapUrls())
                .put("bind_dn", "cn=Thomas Masterman Hardy,ou=people,o=sevenSeas")
                .put("bind_password", PASSWORD)
                .put("group_search.base_dn", groupSearchBase)
                .put("group_search.scope", LdapSearchScope.SUB_TREE)
                .put(LdapSessionFactorySettings.USER_DN_TEMPLATES_SETTING.getKey(), "--")
                .put("ssl.verification_mode", VerificationMode.CERTIFICATE);

        int order = randomIntBetween(0, 10);
        settings.put("order", order);

        boolean userSearch = randomBoolean();
        if (userSearch) {
            settings.put("user_search.base_dn", "");
        }

        RealmConfig config = new RealmConfig("ldap-realm", settings.build(), globalSettings, TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings));

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService, threadPool);
        LdapRealm realm = new LdapRealm(LdapRealmSettings.LDAP_TYPE, config, ldapFactory,
                new DnRoleMapper(config, resourceWatcherService), threadPool);

        PlainActionFuture<Map<String, Object>> future = new PlainActionFuture<>();
        realm.usageStats(future);
        Map<String, Object> stats = future.get();
        assertThat(stats, is(notNullValue()));
        assertThat(stats, hasEntry("name", "ldap-realm"));
        assertThat(stats, hasEntry("order", realm.order()));
        assertThat(stats, hasEntry("size", 0));
        assertThat(stats, hasEntry("ssl", false));
        assertThat(stats, hasEntry("user_search", userSearch));
    }
}
