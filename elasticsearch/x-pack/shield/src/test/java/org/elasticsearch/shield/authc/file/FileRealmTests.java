/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.file;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredStringTests;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.Before;

import java.util.Locale;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 *
 */
public class FileRealmTests extends ESTestCase {

    private FileUserPasswdStore userPasswdStore;
    private FileUserRolesStore userRolesStore;
    private Settings globalSettings;

    @Before
    public void init() throws Exception {
        userPasswdStore = mock(FileUserPasswdStore.class);
        userRolesStore = mock(FileUserRolesStore.class);
        globalSettings = Settings.builder().put("path.home", createTempDir()).build();
    }

    public void testAuthenticate() throws Exception {
        when(userPasswdStore.verifyPassword("user1", SecuredStringTests.build("test123"))).thenReturn(true);
        when(userRolesStore.roles("user1")).thenReturn(new String[] { "role1", "role2" });
        RealmConfig config = new RealmConfig("file-test", Settings.EMPTY, globalSettings);
        FileRealm realm = new FileRealm(config, userPasswdStore, userRolesStore);
        User user = realm.authenticate(new UsernamePasswordToken("user1", SecuredStringTests.build("test123")));
        assertThat(user, notNullValue());
        assertThat(user.principal(), equalTo("user1"));
        assertThat(user.roles(), notNullValue());
        assertThat(user.roles().length, equalTo(2));
        assertThat(user.roles(), arrayContaining("role1", "role2"));
    }

    public void testAuthenticateCaching() throws Exception {
        Settings settings = Settings.builder()
                .put("cache.hash_algo", Hasher.values()[randomIntBetween(0, Hasher.values().length - 1)].name().toLowerCase(Locale.ROOT))
                .build();
        RealmConfig config = new RealmConfig("file-test", settings, globalSettings);
        when(userPasswdStore.verifyPassword("user1", SecuredStringTests.build("test123"))).thenReturn(true);
        when(userRolesStore.roles("user1")).thenReturn(new String[]{"role1", "role2"});
        FileRealm realm = new FileRealm(config, userPasswdStore, userRolesStore);
        User user1 = realm.authenticate(new UsernamePasswordToken("user1", SecuredStringTests.build("test123")));
        User user2 = realm.authenticate(new UsernamePasswordToken("user1", SecuredStringTests.build("test123")));
        assertThat(user1, sameInstance(user2));
    }

    public void testAuthenticateCachingRefresh() throws Exception {
        RealmConfig config = new RealmConfig("file-test", Settings.EMPTY, globalSettings);
        userPasswdStore = spy(new UserPasswdStore(config));
        userRolesStore = spy(new UserRolesStore(config));
        doReturn(true).when(userPasswdStore).verifyPassword("user1", SecuredStringTests.build("test123"));
        doReturn(new String[] { "role1", "role2" }).when(userRolesStore).roles("user1");
        FileRealm realm = new FileRealm(config, userPasswdStore, userRolesStore);
        User user1 = realm.authenticate(new UsernamePasswordToken("user1", SecuredStringTests.build("test123")));
        User user2 = realm.authenticate(new UsernamePasswordToken("user1", SecuredStringTests.build("test123")));
        assertThat(user1, sameInstance(user2));
        userPasswdStore.notifyRefresh();
        User user3 = realm.authenticate(new UsernamePasswordToken("user1", SecuredStringTests.build("test123")));
        assertThat(user2, not(sameInstance(user3)));
        User user4 = realm.authenticate(new UsernamePasswordToken("user1", SecuredStringTests.build("test123")));
        assertThat(user3, sameInstance(user4));
        userRolesStore.notifyRefresh();
        User user5 = realm.authenticate(new UsernamePasswordToken("user1", SecuredStringTests.build("test123")));
        assertThat(user4, not(sameInstance(user5)));
        User user6 = realm.authenticate(new UsernamePasswordToken("user1", SecuredStringTests.build("test123")));
        assertThat(user5, sameInstance(user6));
    }

    public void testToken() throws Exception {
        RealmConfig config = new RealmConfig("file-test", Settings.EMPTY, globalSettings);
        when(userPasswdStore.verifyPassword("user1", SecuredStringTests.build("test123"))).thenReturn(true);
        when(userRolesStore.roles("user1")).thenReturn(new String[]{"role1", "role2"});
        FileRealm realm = new FileRealm(config, userPasswdStore, userRolesStore);

        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        UsernamePasswordToken.putTokenHeader(threadContext, new UsernamePasswordToken("user1", SecuredStringTests.build("test123")));

        UsernamePasswordToken token = realm.token(threadContext);
        assertThat(token, notNullValue());
        assertThat(token.principal(), equalTo("user1"));
        assertThat(token.credentials(), notNullValue());
        assertThat(new String(token.credentials().internalChars()), equalTo("test123"));
    }

    public void testLookup() throws Exception {
        when(userPasswdStore.userExists("user1")).thenReturn(true);
        when(userRolesStore.roles("user1")).thenReturn(new String[] { "role1", "role2" });
        RealmConfig config = new RealmConfig("file-test", Settings.EMPTY, globalSettings);
        FileRealm realm = new FileRealm(config, userPasswdStore, userRolesStore);

        User user = realm.lookupUser("user1");

        assertThat(user, notNullValue());
        assertThat(user.principal(), equalTo("user1"));
        assertThat(user.roles(), notNullValue());
        assertThat(user.roles().length, equalTo(2));
        assertThat(user.roles(), arrayContaining("role1", "role2"));
    }

    public void testLookupCaching() throws Exception {
        when(userPasswdStore.userExists("user1")).thenReturn(true);
        when(userRolesStore.roles("user1")).thenReturn(new String[] { "role1", "role2" });
        RealmConfig config = new RealmConfig("file-test", Settings.EMPTY, globalSettings);
        FileRealm realm = new FileRealm(config, userPasswdStore, userRolesStore);

        User user = realm.lookupUser("user1");
        User user1 = realm.lookupUser("user1");
        assertThat(user, sameInstance(user1));
        verify(userPasswdStore).userExists("user1");
        verify(userRolesStore).roles("user1");
    }

    public void testLookupCachingWithRefresh() throws Exception {
        RealmConfig config = new RealmConfig("file-test", Settings.EMPTY, globalSettings);
        userPasswdStore = spy(new UserPasswdStore(config));
        userRolesStore = spy(new UserRolesStore(config));
        doReturn(true).when(userPasswdStore).userExists("user1");
        doReturn(new String[] { "role1", "role2" }).when(userRolesStore).roles("user1");
        FileRealm realm = new FileRealm(config, userPasswdStore, userRolesStore);
        User user1 = realm.lookupUser("user1");
        User user2 = realm.lookupUser("user1");
        assertThat(user1, sameInstance(user2));
        userPasswdStore.notifyRefresh();
        User user3 = realm.lookupUser("user1");
        assertThat(user2, not(sameInstance(user3)));
        User user4 = realm.lookupUser("user1");
        assertThat(user3, sameInstance(user4));
        userRolesStore.notifyRefresh();
        User user5 = realm.lookupUser("user1");
        assertThat(user4, not(sameInstance(user5)));
        User user6 = realm.lookupUser("user1");
        assertThat(user5, sameInstance(user6));
    }

    static class UserPasswdStore extends FileUserPasswdStore {
        public UserPasswdStore(RealmConfig config) {
            super(config, mock(ResourceWatcherService.class));
        }
    }

    static class UserRolesStore extends FileUserRolesStore {
        public UserRolesStore(RealmConfig config) {
            super(config, mock(ResourceWatcherService.class));
        }
    }
}
