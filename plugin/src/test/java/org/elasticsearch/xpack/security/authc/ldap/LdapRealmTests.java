/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.security.authc.ldap.support.LdapTestCase;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.xpack.security.authc.support.CachingUsernamePasswordRealm;
import org.elasticsearch.xpack.security.authc.support.DnRoleMapper;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.elasticsearch.xpack.security.authc.support.SecuredStringTests;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.ssl.SSLService;
import org.elasticsearch.xpack.ssl.VerificationMode;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Map;

import static org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory.URLS_SETTING;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class LdapRealmTests extends LdapTestCase {

    public static final String VALID_USER_TEMPLATE = "cn={0},ou=people,o=sevenSeas";
    public static final String VALID_USERNAME = "Thomas Masterman Hardy";
    public static final String PASSWORD = "pass";

    private static final String USER_DN_TEMPLATES_SETTING_KEY = LdapSessionFactory.USER_DN_TEMPLATES_SETTING.getKey();

    private ThreadPool threadPool;
    private ResourceWatcherService resourceWatcherService;
    private Settings globalSettings;
    private SSLService sslService;

    @Before
    public void init() throws Exception {
        threadPool = new TestThreadPool("ldap realm tests");
        resourceWatcherService = new ResourceWatcherService(Settings.EMPTY, threadPool);
        globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        sslService = new SSLService(globalSettings, new Environment(globalSettings));
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
        RealmConfig config = new RealmConfig("test-ldap-realm", settings, globalSettings);
        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService);
        LdapRealm ldap = new LdapRealm(LdapRealm.LDAP_TYPE, config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService), 
                threadPool);

        PlainActionFuture<User> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)), future);
        User user = future.actionGet();
        assertThat(user, notNullValue());
        assertThat(user.roles(), arrayContaining("HMS Victory"));
    }

    public void testAuthenticateOneLevelGroupSearch() throws Exception {
        String groupSearchBase = "ou=crews,ou=groups,o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = Settings.builder()
                .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.ONE_LEVEL))
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm", settings, globalSettings);

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService);
        LdapRealm ldap =
                new LdapRealm(LdapRealm.LDAP_TYPE, config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService), threadPool);

        PlainActionFuture<User> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)), future);
        User user = future.actionGet();
        assertThat(user, notNullValue());
        assertThat("For roles " + Arrays.toString(user.roles()), user.roles(), arrayContaining("HMS Victory"));
    }

    public void testAuthenticateCaching() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = Settings.builder()
                .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm", settings, globalSettings);

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService);
        ldapFactory = spy(ldapFactory);
        LdapRealm ldap =
                new LdapRealm(LdapRealm.LDAP_TYPE, config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService), threadPool);
        PlainActionFuture<User> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)), future);
        future.actionGet();
        future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)), future);
        future.actionGet();

        //verify one and only one session -> caching is working
        verify(ldapFactory, times(1)).session(anyString(), any(SecuredString.class), any(ActionListener.class));
    }

    public void testAuthenticateCachingRefresh() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = Settings.builder()
                .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm", settings, globalSettings);

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService);
        DnRoleMapper roleMapper = buildGroupAsRoleMapper(resourceWatcherService);
        ldapFactory = spy(ldapFactory);
        LdapRealm ldap = new LdapRealm(LdapRealm.LDAP_TYPE, config, ldapFactory, roleMapper, threadPool);
        PlainActionFuture<User> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)), future);
        future.actionGet();
        future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)), future);
        future.actionGet();

        //verify one and only one session -> caching is working
        verify(ldapFactory, times(1)).session(anyString(), any(SecuredString.class), any(ActionListener.class));

        roleMapper.notifyRefresh();

        future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)), future);
        future.actionGet();

        //we need to session again
        verify(ldapFactory, times(2)).session(anyString(), any(SecuredString.class), any(ActionListener.class));
    }

    public void testAuthenticateNoncaching() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = Settings.builder()
                .put(buildLdapSettings(ldapUrls(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put(CachingUsernamePasswordRealm.CACHE_TTL_SETTING.getKey(), -1)
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm", settings, globalSettings);

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService);
        ldapFactory = spy(ldapFactory);
        LdapRealm ldap =
                new LdapRealm(LdapRealm.LDAP_TYPE, config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService), threadPool);
        PlainActionFuture<User> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)), future);
        future.actionGet();
        future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)), future);
        future.actionGet();

        //verify two and only two binds -> caching is disabled
        verify(ldapFactory, times(2)).session(anyString(), any(SecuredString.class), any(ActionListener.class));
    }

    public void testLdapRealmSelectsLdapSessionFactory() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = Settings.builder()
                .putArray(URLS_SETTING, ldapUrls())
                .putArray(USER_DN_TEMPLATES_SETTING_KEY, userTemplate)
                .put("group_search.base_dn", groupSearchBase)
                .put("group_search.scope", LdapSearchScope.SUB_TREE)
                .put("ssl.verification_mode", VerificationMode.CERTIFICATE)
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm", settings, globalSettings);
        SessionFactory sessionFactory = LdapRealm.sessionFactory(config, sslService, LdapRealm.LDAP_TYPE);
        assertThat(sessionFactory, is(instanceOf(LdapSessionFactory.class)));
    }

    public void testLdapRealmSelectsLdapUserSearchSessionFactory() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        Settings settings = Settings.builder()
                .putArray(URLS_SETTING, ldapUrls())
                .put("user_search.base_dn", "")
                .put("bind_dn", "cn=Thomas Masterman Hardy,ou=people,o=sevenSeas")
                .put("bind_password", PASSWORD)
                .put("group_search.base_dn", groupSearchBase)
                .put("group_search.scope", LdapSearchScope.SUB_TREE)
                .put("ssl.verification_mode", VerificationMode.CERTIFICATE)
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm-user-search", settings, globalSettings);
        SessionFactory sessionFactory = LdapRealm.sessionFactory(config, sslService, LdapRealm.LDAP_TYPE);
        try {
            assertThat(sessionFactory, is(instanceOf(LdapUserSearchSessionFactory.class)));
        } finally {
            ((LdapUserSearchSessionFactory)sessionFactory).shutdown();
        }
    }

    public void testLdapRealmThrowsExceptionForUserTemplateAndSearchSettings() throws Exception {
        Settings settings = Settings.builder()
                .putArray(URLS_SETTING, ldapUrls())
                .putArray(USER_DN_TEMPLATES_SETTING_KEY, "cn=foo")
                .put("user_search.base_dn", "cn=bar")
                .put("group_search.base_dn", "")
                .put("group_search.scope", LdapSearchScope.SUB_TREE)
                .put("ssl.verification_mode", VerificationMode.CERTIFICATE)
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm-user-search", settings, globalSettings);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> LdapRealm.sessionFactory(config, null, LdapRealm.LDAP_TYPE));
        assertThat(e.getMessage(),
                containsString("settings were found for both" +
                        " user search [xpack.security.authc.realms.test-ldap-realm-user-search.user_search.] and" +
                        " user template [xpack.security.authc.realms.test-ldap-realm-user-search.user_dn_templates]"));
    }

    public void testLdapRealmThrowsExceptionWhenNeitherUserTemplateNorSearchSettingsProvided() throws Exception {
        Settings settings = Settings.builder()
                .putArray(URLS_SETTING, ldapUrls())
                .put("group_search.base_dn", "")
                .put("group_search.scope", LdapSearchScope.SUB_TREE)
                .put("ssl.verification_mode", VerificationMode.CERTIFICATE)
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm-user-search", settings, globalSettings);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> LdapRealm.sessionFactory(config, null, LdapRealm.LDAP_TYPE));
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
                .put(DnRoleMapper.ROLE_MAPPING_FILE_SETTING.getKey(),
                        getDataPath("/org/elasticsearch/xpack/security/authc/support/role_mapping.yml"))
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm-userdn", settings, globalSettings);

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService);
        LdapRealm ldap = new LdapRealm(LdapRealm.LDAP_TYPE, config, ldapFactory,
                new DnRoleMapper(LdapRealm.LDAP_TYPE, config, resourceWatcherService, null), threadPool);

        PlainActionFuture<User> future = new PlainActionFuture<>();
        ldap.authenticate(new UsernamePasswordToken("Horatio Hornblower", SecuredStringTests.build(PASSWORD)), future);
        User user = future.actionGet();
        assertThat(user, notNullValue());
        assertThat(user.roles(), arrayContaining("avenger"));
    }

    public void testUsageStats() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        Settings.Builder settings = Settings.builder()
                .putArray(URLS_SETTING, ldapUrls())
                .put("bind_dn", "cn=Thomas Masterman Hardy,ou=people,o=sevenSeas")
                .put("bind_password", PASSWORD)
                .put("group_search.base_dn", groupSearchBase)
                .put("group_search.scope", LdapSearchScope.SUB_TREE)
                .put(LdapSessionFactory.USER_DN_TEMPLATES_SETTING.getKey(), "--")
                .put("ssl.verification_mode", VerificationMode.CERTIFICATE);

        int order = randomIntBetween(0, 10);
        settings.put("order", order);

        boolean userSearch = randomBoolean();
        if (userSearch) {
            settings.put("user_search.base_dn", "");
        }

        RealmConfig config = new RealmConfig("ldap-realm", settings.build(), globalSettings);

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, sslService);
        LdapRealm realm = new LdapRealm(LdapRealm.LDAP_TYPE, config, ldapFactory,
                new DnRoleMapper(LdapRealm.LDAP_TYPE, config, resourceWatcherService, null), threadPool);

        Map<String, Object> stats = realm.usageStats();
        assertThat(stats, is(notNullValue()));
        assertThat(stats, hasEntry("name", "ldap-realm"));
        assertThat(stats, hasEntry("order", realm.order()));
        assertThat(stats, hasEntry("size", 0));
        assertThat(stats, hasEntry("ssl", false));
        assertThat(stats, hasEntry("user_search", userSearch));
    }
}
