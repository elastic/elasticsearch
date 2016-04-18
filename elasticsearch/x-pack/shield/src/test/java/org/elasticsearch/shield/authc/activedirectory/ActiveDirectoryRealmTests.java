/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.activedirectory;

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;
import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPURL;
import com.unboundid.ldap.sdk.schema.Schema;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.CachingUsernamePasswordRealm;
import org.elasticsearch.shield.authc.support.DnRoleMapper;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.SecuredStringTests;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.shield.authc.ldap.support.SessionFactory.HOSTNAME_VERIFICATION_SETTING;
import static org.elasticsearch.shield.authc.ldap.support.SessionFactory.URLS_SETTING;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Active Directory Realm tests that use the UnboundID In Memory Directory Server
 *
 * AD is not LDAPv3 compliant so a workaround is needed
 * AD realm binds with userPrincipalName but this is not a valid DN, so we have to add a second userPrincipalName to the
 * users in the ldif in the form of CN=user@domain.com or a set the sAMAccountName to CN=user when testing authentication
 * with the sAMAccountName field.
 *
 * The username used to authenticate then has to be in the form of CN=user. Finally the username needs to be added as an
 * additional bind DN with a password in the test setup since it really is not a DN in the ldif file
 */
public class ActiveDirectoryRealmTests extends ESTestCase {
    private static final String PASSWORD = "password";

    protected static int numberOfLdapServers;
    protected InMemoryDirectoryServer[] directoryServers;

    private ResourceWatcherService resourceWatcherService;
    private ThreadPool threadPool;
    private Settings globalSettings;

    @BeforeClass
    public static void setNumberOfLdapServers() {
        numberOfLdapServers = randomIntBetween(1, 4);
    }

    @Before
    public void start() throws Exception {
        InMemoryDirectoryServerConfig config = new InMemoryDirectoryServerConfig("dc=ad,dc=test,dc=elasticsearch,dc=com");
        // Get the default schema and overlay with the AD changes
        config.setSchema(Schema.mergeSchemas(Schema.getDefaultStandardSchema(),
                Schema.getSchema(getDataPath("ad-schema.ldif").toString())));

        // Add the bind users here since AD is not LDAPv3 compliant
        config.addAdditionalBindCredentials("CN=ironman@ad.test.elasticsearch.com", PASSWORD);
        config.addAdditionalBindCredentials("CN=Thor@ad.test.elasticsearch.com", PASSWORD);

        directoryServers = new InMemoryDirectoryServer[numberOfLdapServers];
        for (int i = 0; i < numberOfLdapServers; i++) {
            InMemoryDirectoryServer directoryServer = new InMemoryDirectoryServer(config);
            directoryServer.add("dc=ad,dc=test,dc=elasticsearch,dc=com", new Attribute("dc", "UnboundID"),
                    new Attribute("objectClass", "top", "domain", "extensibleObject"));
            directoryServer.importFromLDIF(false, getDataPath("ad.ldif").toString());
            directoryServer.startListening();
            directoryServers[i] = directoryServer;
        }
        threadPool = new ThreadPool("active directory realm tests");
        resourceWatcherService = new ResourceWatcherService(Settings.EMPTY, threadPool);
        globalSettings = Settings.builder().put("path.home", createTempDir()).build();
    }

    @After
    public void stop() throws InterruptedException {
        resourceWatcherService.stop();
        terminate(threadPool);
        for (int i = 0; i < numberOfLdapServers; i++) {
            directoryServers[i].shutDown(true);
        }
    }

    public void testAuthenticateUserPrincipleName() throws Exception {
        Settings settings = settings();
        RealmConfig config = new RealmConfig("testAuthenticateUserPrincipleName", settings, globalSettings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, null).init();
        DnRoleMapper roleMapper = new DnRoleMapper(ActiveDirectoryRealm.TYPE, config, resourceWatcherService, null);
        ActiveDirectoryRealm realm = new ActiveDirectoryRealm(config, sessionFactory, roleMapper);

        User user = realm.authenticate(new UsernamePasswordToken("CN=ironman", SecuredStringTests.build(PASSWORD)));
        assertThat(user, is(notNullValue()));
        assertThat(user.roles(), arrayContaining(containsString("Avengers")));
    }

    public void testAuthenticateSAMAccountName() throws Exception {
        Settings settings = settings();
        RealmConfig config = new RealmConfig("testAuthenticateSAMAccountName", settings, globalSettings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, null).init();
        DnRoleMapper roleMapper = new DnRoleMapper(ActiveDirectoryRealm.TYPE, config, resourceWatcherService, null);
        ActiveDirectoryRealm realm = new ActiveDirectoryRealm(config, sessionFactory, roleMapper);

        // Thor does not have a UPN of form CN=Thor@ad.test.elasticsearch.com
        User user = realm.authenticate(new UsernamePasswordToken("CN=Thor", SecuredStringTests.build(PASSWORD)));
        assertThat(user, is(notNullValue()));
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

    public void testAuthenticateCachesSuccesfulAuthentications() throws Exception {
        Settings settings = settings();
        RealmConfig config = new RealmConfig("testAuthenticateCachesSuccesfulAuthentications", settings, globalSettings);
        ActiveDirectorySessionFactory sessionFactory = spy(new ActiveDirectorySessionFactory(config, null).init());
        DnRoleMapper roleMapper = new DnRoleMapper(ActiveDirectoryRealm.TYPE, config, resourceWatcherService, null);
        ActiveDirectoryRealm realm = new ActiveDirectoryRealm(config, sessionFactory, roleMapper);

        int count = randomIntBetween(2, 10);
        for (int i = 0; i < count; i++) {
            realm.authenticate(new UsernamePasswordToken("CN=ironman", SecuredStringTests.build(PASSWORD)));
        }

        // verify one and only one session as further attempts should be returned from cache
        verify(sessionFactory, times(1)).session(eq("CN=ironman"), any(SecuredString.class));
    }

    public void testAuthenticateCachingCanBeDisabled() throws Exception {
        Settings settings = settings(Settings.builder().put(CachingUsernamePasswordRealm.CACHE_TTL_SETTING, -1).build());
        RealmConfig config = new RealmConfig("testAuthenticateCachingCanBeDisabled", settings, globalSettings);
        ActiveDirectorySessionFactory sessionFactory = spy(new ActiveDirectorySessionFactory(config, null).init());
        DnRoleMapper roleMapper = new DnRoleMapper(ActiveDirectoryRealm.TYPE, config, resourceWatcherService, null);
        ActiveDirectoryRealm realm = new ActiveDirectoryRealm(config, sessionFactory, roleMapper);

        int count = randomIntBetween(2, 10);
        for (int i = 0; i < count; i++) {
            realm.authenticate(new UsernamePasswordToken("CN=ironman", SecuredStringTests.build(PASSWORD)));
        }

        // verify one and only one session as second attempt should be returned from cache
        verify(sessionFactory, times(count)).session(eq("CN=ironman"), any(SecuredString.class));
    }

    public void testAuthenticateCachingClearsCacheOnRoleMapperRefresh() throws Exception {
        Settings settings = settings();
        RealmConfig config = new RealmConfig("testAuthenticateCachingClearsCacheOnRoleMapperRefresh", settings, globalSettings);
        ActiveDirectorySessionFactory sessionFactory = spy(new ActiveDirectorySessionFactory(config, null).init());
        DnRoleMapper roleMapper = new DnRoleMapper(ActiveDirectoryRealm.TYPE, config, resourceWatcherService, null);
        ActiveDirectoryRealm realm = new ActiveDirectoryRealm(config, sessionFactory, roleMapper);

        int count = randomIntBetween(2, 10);
        for (int i = 0; i < count; i++) {
            realm.authenticate(new UsernamePasswordToken("CN=ironman", SecuredStringTests.build(PASSWORD)));
        }

        // verify one and only one session as further attempts should be returned from cache
        verify(sessionFactory, times(1)).session(eq("CN=ironman"), any(SecuredString.class));

        // Refresh the role mappings
        roleMapper.notifyRefresh();

        for (int i = 0; i < count; i++) {
            realm.authenticate(new UsernamePasswordToken("CN=ironman", SecuredStringTests.build(PASSWORD)));
        }

        verify(sessionFactory, times(2)).session(eq("CN=ironman"), any(SecuredString.class));
    }

    public void testRealmMapsGroupsToRoles() throws Exception {
        Settings settings = settings(Settings.builder()
                .put(DnRoleMapper.ROLE_MAPPING_FILE_SETTING, getDataPath("role_mapping.yml"))
                .build());
        RealmConfig config = new RealmConfig("testRealmMapsGroupsToRoles", settings, globalSettings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, null).init();
        DnRoleMapper roleMapper = new DnRoleMapper(ActiveDirectoryRealm.TYPE, config, resourceWatcherService, null);
        ActiveDirectoryRealm realm = new ActiveDirectoryRealm(config, sessionFactory, roleMapper);

        User user = realm.authenticate(new UsernamePasswordToken("CN=ironman", SecuredStringTests.build(PASSWORD)));
        assertThat(user, is(notNullValue()));
        assertThat(user.roles(), arrayContaining(equalTo("group_role")));
    }

    public void testRealmMapsUsersToRoles() throws Exception {
        Settings settings = settings(Settings.builder()
                .put(DnRoleMapper.ROLE_MAPPING_FILE_SETTING, getDataPath("role_mapping.yml"))
                .build());
        RealmConfig config = new RealmConfig("testRealmMapsGroupsToRoles", settings, globalSettings);
        ActiveDirectorySessionFactory sessionFactory = new ActiveDirectorySessionFactory(config, null).init();
        DnRoleMapper roleMapper = new DnRoleMapper(ActiveDirectoryRealm.TYPE, config, resourceWatcherService, null);
        ActiveDirectoryRealm realm = new ActiveDirectoryRealm(config, sessionFactory, roleMapper);

        User user = realm.authenticate(new UsernamePasswordToken("CN=Thor", SecuredStringTests.build(PASSWORD)));
        assertThat(user, is(notNullValue()));
        assertThat(user.roles(), arrayContainingInAnyOrder(equalTo("group_role"), equalTo("user_role")));
    }

    private Settings settings() throws Exception {
        return settings(Settings.EMPTY);
    }

    private Settings settings(Settings extraSettings) throws Exception {
        return Settings.builder()
                .putArray(URLS_SETTING, ldapUrls())
                .put(ActiveDirectorySessionFactory.AD_DOMAIN_NAME_SETTING, "ad.test.elasticsearch.com")
                .put(DnRoleMapper.USE_UNMAPPED_GROUPS_AS_ROLES_SETTING, true)
                .put(HOSTNAME_VERIFICATION_SETTING, false)
                .put(extraSettings)
                .build();
    }
}
