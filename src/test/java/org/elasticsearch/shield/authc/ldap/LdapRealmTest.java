/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.shield.ShieldSettingsException;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.ldap.support.SessionFactory;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.SecuredStringTests;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.authc.ldap.support.LdapRoleMapper;
import org.elasticsearch.shield.authc.ldap.support.LdapTest;
import org.elasticsearch.shield.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.shield.authc.ldap.LdapSessionFactory.USER_DN_TEMPLATES_SETTING;
import static org.elasticsearch.shield.authc.ldap.support.SessionFactory.HOSTNAME_VERIFICATION_SETTING;
import static org.elasticsearch.shield.authc.ldap.support.SessionFactory.URLS_SETTING;
import static org.hamcrest.Matchers.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class LdapRealmTest extends LdapTest {

    public static final String VALID_USER_TEMPLATE = "cn={0},ou=people,o=sevenSeas";
    public static final String VALID_USERNAME = "Thomas Masterman Hardy";
    public static final String PASSWORD = "pass";

    private RestController restController;
    private ThreadPool threadPool;
    private ResourceWatcherService resourceWatcherService;

    @Before
    public void init() throws Exception {
        restController = mock(RestController.class);
        threadPool = new ThreadPool("test");
        resourceWatcherService = new ResourceWatcherService(ImmutableSettings.EMPTY, threadPool);
    }

    @After
    public void shutdown() throws InterruptedException {
        resourceWatcherService.stop();
        terminate(threadPool);
    }

    @Test
    public void testRestHeaderRegistration() {
        new LdapRealm.Factory(resourceWatcherService, restController, null);
        verify(restController).registerRelevantHeaders(UsernamePasswordToken.BASIC_AUTH_HEADER);
    }

    @Test
    public void testAuthenticate_SubTreeGroupSearch() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = buildLdapSettings(ldapUrl(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE);
        RealmConfig config = new RealmConfig("test-ldap-realm", settings);
        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, null);
        LdapRealm ldap = new LdapRealm(config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService));

        User user = ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)));
        assertThat(user, notNullValue());
        assertThat(user.roles(), arrayContaining("HMS Victory"));
    }

    @Test
    public void testAuthenticate_OneLevelGroupSearch() throws Exception {
        String groupSearchBase = "ou=crews,ou=groups,o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = ImmutableSettings.builder()
                .put(buildLdapSettings(ldapUrl(), userTemplate, groupSearchBase, LdapSearchScope.ONE_LEVEL))
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm", settings);

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, null);
        LdapRealm ldap = new LdapRealm(config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService));

        User user = ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)));
        assertThat(user, notNullValue());
        assertThat(user.roles(), arrayContaining("HMS Victory"));
    }

    @Test
    public void testAuthenticate_Caching() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = ImmutableSettings.builder()
                .put(buildLdapSettings(ldapUrl(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm", settings);

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, null);
        ldapFactory = spy(ldapFactory);
        LdapRealm ldap = new LdapRealm(config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService));
        ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)));
        ldap.authenticate( new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)));

        //verify one and only one session -> caching is working
        verify(ldapFactory, times(1)).session(anyString(), any(SecuredString.class));
    }

    @Test
    public void testAuthenticate_Caching_Refresh() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = ImmutableSettings.builder()
                .put(buildLdapSettings(ldapUrl(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm", settings);

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, null);
        LdapRoleMapper roleMapper = buildGroupAsRoleMapper(resourceWatcherService);
        ldapFactory = spy(ldapFactory);
        LdapRealm ldap = new LdapRealm(config, ldapFactory, roleMapper);
        ldap.authenticate( new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)));
        ldap.authenticate( new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)));

        //verify one and only one session -> caching is working
        verify(ldapFactory, times(1)).session(anyString(), any(SecuredString.class));

        roleMapper.notifyRefresh();

        ldap.authenticate( new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)));

        //we need to session again
        verify(ldapFactory, times(2)).session(anyString(), any(SecuredString.class));
    }

    @Test
    public void testAuthenticate_Noncaching() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = ImmutableSettings.builder()
                .put(buildLdapSettings(ldapUrl(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put(LdapRealm.CACHE_TTL_SETTING, -1)
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm", settings);

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, null);
        ldapFactory = spy(ldapFactory);
        LdapRealm ldap = new LdapRealm(config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService));
        ldap.authenticate( new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)));
        ldap.authenticate( new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)));

        //verify two and only two binds -> caching is disabled
        verify(ldapFactory, times(2)).session(anyString(), any(SecuredString.class));
    }

    @Test
    public void testLdapRealmSelectsLdapSessionFactory() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = ImmutableSettings.builder()
                .putArray(URLS_SETTING, ldapUrl())
                .putArray(USER_DN_TEMPLATES_SETTING, userTemplate)
                .put("group_search.base_dn", groupSearchBase)
                .put("group_search.scope", LdapSearchScope.SUB_TREE)
                .put(HOSTNAME_VERIFICATION_SETTING, false)
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm", settings);
        SessionFactory sessionFactory = LdapRealm.Factory.sessionFactory(config, null);
        assertThat(sessionFactory, is(instanceOf(LdapSessionFactory.class)));
    }

    @Test
    public void testLdapRealmSelectsLdapUserSearchSessionFactory() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        Settings settings = ImmutableSettings.builder()
                .putArray(URLS_SETTING, ldapUrl())
                .put("user_search.base_dn", "")
                .put("bind_dn", "cn=Thomas Masterman Hardy,ou=people,o=sevenSeas")
                .put("bind_password", PASSWORD)
                .put("group_search.base_dn", groupSearchBase)
                .put("group_search.scope", LdapSearchScope.SUB_TREE)
                .put(HOSTNAME_VERIFICATION_SETTING, false)
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm-user-search", settings);
        SessionFactory sessionFactory = LdapRealm.Factory.sessionFactory(config, null);
        try {
            assertThat(sessionFactory, is(instanceOf(LdapUserSearchSessionFactory.class)));
        } finally {
            ((LdapUserSearchSessionFactory)sessionFactory).shutdown();
        }
    }

    @Test
    public void testLdapRealmThrowsExceptionForUserTemplateAndSearchSettings() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .putArray(URLS_SETTING, ldapUrl())
                .putArray(USER_DN_TEMPLATES_SETTING, "cn=foo")
                .put("user_search.base_dn", "cn=bar")
                .put("group_search.base_dn", "")
                .put("group_search.scope", LdapSearchScope.SUB_TREE)
                .put(HOSTNAME_VERIFICATION_SETTING, false)
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm-user-search", settings);
        try {
            LdapRealm.Factory.sessionFactory(config, null);
            fail("an exception should have been thrown because both user template and user search settings were specified");
        } catch (ShieldSettingsException e) {
            assertThat(e.getMessage(), containsString("settings were found for both user search and user template"));
        }
    }

    @Test
    public void testLdapRealmMapsUserDNToRole() throws Exception {
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = ImmutableSettings.builder()
                .put(buildLdapSettings(ldapUrl(), userTemplate, groupSearchBase, LdapSearchScope.SUB_TREE))
                .put(LdapRoleMapper.ROLE_MAPPING_FILE_SETTING, getResource("support/role_mapping.yml").getCanonicalPath())
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm-userdn", settings);

        LdapSessionFactory ldapFactory = new LdapSessionFactory(config, null);
        LdapRealm ldap = new LdapRealm(config, ldapFactory, new LdapRoleMapper(LdapRealm.TYPE, config, resourceWatcherService, null));

        User user = ldap.authenticate(new UsernamePasswordToken("Horatio Hornblower", SecuredStringTests.build(PASSWORD)));
        assertThat(user, notNullValue());
        assertThat(user.roles(), arrayContaining("avenger"));
    }
}
