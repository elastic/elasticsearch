/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.SecuredStringTests;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.authc.support.ldap.LdapTest;
import org.elasticsearch.shield.authc.support.ldap.SearchScope;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.notNullValue;
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
    public void shutdown() {
        resourceWatcherService.stop();
        threadPool.shutdownNow();
    }

    @Test
    public void testRestHeaderRegistration() {
        new LdapRealm.Factory(resourceWatcherService, restController);
        verify(restController).registerRelevantHeaders(UsernamePasswordToken.BASIC_AUTH_HEADER);
    }

    @Test
    public void testAuthenticate_SubTreeGroupSearch(){
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = buildLdapSettings(ldapUrl(), userTemplate, groupSearchBase, SearchScope.SUB_TREE);
        RealmConfig config = new RealmConfig("test-ldap-realm", settings);
        LdapConnectionFactory ldapFactory = new LdapConnectionFactory(config);
        LdapRealm ldap = new LdapRealm(config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService));

        User user = ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)));
        assertThat( user, notNullValue());
        assertThat(user.roles(), arrayContaining("HMS Victory"));
    }

    @Test
    public void testAuthenticate_OneLevelGroupSearch(){
        String groupSearchBase = "ou=crews,ou=groups,o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = ImmutableSettings.builder()
                .put(buildLdapSettings(ldapUrl(), userTemplate, groupSearchBase, SearchScope.ONE_LEVEL))
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm", settings);

        LdapConnectionFactory ldapFactory = new LdapConnectionFactory(config);
        LdapRealm ldap = new LdapRealm(config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService));

        User user = ldap.authenticate(new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)));
        assertThat( user, notNullValue());
        assertThat(user.roles(), arrayContaining("HMS Victory"));
    }

    @Test
    public void testAuthenticate_Caching(){
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = ImmutableSettings.builder()
                .put(buildLdapSettings(ldapUrl(), userTemplate, groupSearchBase, SearchScope.SUB_TREE))
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm", settings);

        LdapConnectionFactory ldapFactory = new LdapConnectionFactory(config);
        ldapFactory = spy(ldapFactory);
        LdapRealm ldap = new LdapRealm(config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService));
        ldap.authenticate( new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)));
        ldap.authenticate( new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)));

        //verify one and only one open -> caching is working
        verify(ldapFactory, times(1)).open(anyString(), any(SecuredString.class));
    }

    @Test
    public void testAuthenticate_Caching_Refresh(){
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = ImmutableSettings.builder()
                .put(buildLdapSettings(ldapUrl(), userTemplate, groupSearchBase, SearchScope.SUB_TREE))
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm", settings);

        LdapConnectionFactory ldapFactory = new LdapConnectionFactory(config);
        LdapGroupToRoleMapper roleMapper = buildGroupAsRoleMapper(resourceWatcherService);
        ldapFactory = spy(ldapFactory);
        LdapRealm ldap = new LdapRealm(config, ldapFactory, roleMapper);
        ldap.authenticate( new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)));
        ldap.authenticate( new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)));

        //verify one and only one open -> caching is working
        verify(ldapFactory, times(1)).open(anyString(), any(SecuredString.class));

        roleMapper.notifyRefresh();

        ldap.authenticate( new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)));

        //we need to open again
        verify(ldapFactory, times(2)).open(anyString(), any(SecuredString.class));
    }

    @Test
    public void testAuthenticate_Noncaching(){
        String groupSearchBase = "o=sevenSeas";
        String userTemplate = VALID_USER_TEMPLATE;
        Settings settings = ImmutableSettings.builder()
                .put(buildLdapSettings(ldapUrl(), userTemplate, groupSearchBase, SearchScope.SUB_TREE))
                .put(LdapRealm.CACHE_TTL, -1)
                .build();
        RealmConfig config = new RealmConfig("test-ldap-realm", settings);

        LdapConnectionFactory ldapFactory = new LdapConnectionFactory(config);
        ldapFactory = spy(ldapFactory);
        LdapRealm ldap = new LdapRealm(config, ldapFactory, buildGroupAsRoleMapper(resourceWatcherService));
        ldap.authenticate( new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)));
        ldap.authenticate( new UsernamePasswordToken(VALID_USERNAME, SecuredStringTests.build(PASSWORD)));

        //verify two and only two binds -> caching is disabled
        verify(ldapFactory, times(2)).open(anyString(), any(SecuredString.class));
    }

}
