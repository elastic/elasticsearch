/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.file;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.test.SecurityIntegTestCase.getFastStoredHashAlgoForTests;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FileRealmTests extends ESTestCase {

    private static final RealmConfig.RealmIdentifier REALM_IDENTIFIER = new RealmConfig.RealmIdentifier("file", "file-test");

    private static final Answer<AuthenticationResult> VERIFY_PASSWORD_ANSWER = inv -> {
        assertThat(inv.getArguments().length, is(3));
        @SuppressWarnings("unchecked")
        Supplier<User> supplier = (Supplier<User>) inv.getArguments()[2];
        return AuthenticationResult.success(supplier.get());
    };

    private FileUserPasswdStore userPasswdStore;
    private FileUserRolesStore userRolesStore;
    private Settings globalSettings;
    private ThreadPool threadPool;
    private ThreadContext threadContext;

    @Before
    public void init() throws Exception {
        userPasswdStore = mock(FileUserPasswdStore.class);
        userRolesStore = mock(FileUserRolesStore.class);
        globalSettings = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.authc.password_hashing.algorithm", getFastStoredHashAlgoForTests().name())
            .put(RealmSettings.realmSettingPrefix(REALM_IDENTIFIER) + "order", 0).build();
        threadPool = mock(ThreadPool.class);
        threadContext = new ThreadContext(globalSettings);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
    }

    public void testAuthenticate() throws Exception {
        when(userPasswdStore.verifyPassword(eq("user1"), eq(new SecureString("longtestpassword")), anySupplier()))
                .thenAnswer(VERIFY_PASSWORD_ANSWER);
        when(userRolesStore.roles("user1")).thenReturn(new String[] { "role1", "role2" });
        RealmConfig config = getRealmConfig(globalSettings);
        FileRealm realm = new FileRealm(config, userPasswdStore, userRolesStore, threadPool);
        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("user1", new SecureString("longtestpassword")), future);
        final AuthenticationResult result = future.actionGet();
        assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
        User user = result.getUser();
        assertThat(user, notNullValue());
        assertThat(user.principal(), equalTo("user1"));
        assertThat(user.roles(), notNullValue());
        assertThat(user.roles().length, equalTo(2));
        assertThat(user.roles(), arrayContaining("role1", "role2"));
    }

    private RealmConfig getRealmConfig(Settings settings) {
        return new RealmConfig(REALM_IDENTIFIER, settings, TestEnvironment.newEnvironment(settings), threadContext);
    }

    public void testAuthenticateCaching() throws Exception {
        Settings settings = Settings.builder()
            .put(RealmSettings.realmSettingPrefix(REALM_IDENTIFIER) + "cache.hash_algo",
                randomFrom(Hasher.getAvailableAlgoCacheHash()))
            .put(globalSettings)
            .build();
        RealmConfig config = getRealmConfig(settings);
        when(userPasswdStore.verifyPassword(eq("user1"), eq(new SecureString("longtestpassword")), anySupplier()))
                .thenAnswer(VERIFY_PASSWORD_ANSWER);
        when(userRolesStore.roles("user1")).thenReturn(new String[]{"role1", "role2"});
        FileRealm realm = new FileRealm(config, userPasswdStore, userRolesStore, threadPool);
        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("user1", new SecureString("longtestpassword")), future);
        User user1 = future.actionGet().getUser();
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("user1", new SecureString("longtestpassword")), future);
        User user2 = future.actionGet().getUser();
        assertThat(user1, sameInstance(user2));
    }

    public void testAuthenticateCachingRefresh() throws Exception {
        RealmConfig config = getRealmConfig(globalSettings);
        userPasswdStore = spy(new UserPasswdStore(config));
        userRolesStore = spy(new UserRolesStore(config));
        when(userPasswdStore.verifyPassword(eq("user1"), eq(new SecureString("longtestpassword")), anySupplier()))
                .thenAnswer(VERIFY_PASSWORD_ANSWER);
        doReturn(new String[] { "role1", "role2" }).when(userRolesStore).roles("user1");
        FileRealm realm = new FileRealm(config, userPasswdStore, userRolesStore, threadPool);
        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("user1", new SecureString("longtestpassword")), future);
        User user1 = future.actionGet().getUser();
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("user1", new SecureString("longtestpassword")), future);
        User user2 = future.actionGet().getUser();
        assertThat(user1, sameInstance(user2));

        userPasswdStore.notifyRefresh();

        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("user1", new SecureString("longtestpassword")), future);
        User user3 = future.actionGet().getUser();
        assertThat(user2, not(sameInstance(user3)));
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("user1", new SecureString("longtestpassword")), future);
        User user4 = future.actionGet().getUser();
        assertThat(user3, sameInstance(user4));

        userRolesStore.notifyRefresh();

        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("user1", new SecureString("longtestpassword")), future);
        User user5 = future.actionGet().getUser();
        assertThat(user4, not(sameInstance(user5)));
        future = new PlainActionFuture<>();
        realm.authenticate(new UsernamePasswordToken("user1", new SecureString("longtestpassword")), future);
        User user6 = future.actionGet().getUser();
        assertThat(user5, sameInstance(user6));
    }

    public void testToken() throws Exception {
        RealmConfig config = getRealmConfig(globalSettings);
        when(userPasswdStore.verifyPassword(eq("user1"), eq(new SecureString("longtestpassword")), anySupplier()))
            .thenAnswer(VERIFY_PASSWORD_ANSWER);
        when(userRolesStore.roles("user1")).thenReturn(new String[]{"role1", "role2"});
        FileRealm realm = new FileRealm(config, userPasswdStore, userRolesStore, threadPool);

        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        UsernamePasswordToken.putTokenHeader(threadContext,
            new UsernamePasswordToken("user1", new SecureString("longtestpassword")));

        UsernamePasswordToken token = realm.token(threadContext);
        assertThat(token, notNullValue());
        assertThat(token.principal(), equalTo("user1"));
        assertThat(token.credentials(), notNullValue());
        assertThat(new String(token.credentials().getChars()), equalTo("longtestpassword"));
    }

    public void testLookup() throws Exception {
        when(userPasswdStore.userExists("user1")).thenReturn(true);
        when(userRolesStore.roles("user1")).thenReturn(new String[] { "role1", "role2" });
        RealmConfig config = getRealmConfig(globalSettings);
        FileRealm realm = new FileRealm(config, userPasswdStore, userRolesStore, threadPool);

        PlainActionFuture<User> future = new PlainActionFuture<>();
        realm.lookupUser("user1", future);
        User user = future.actionGet();

        assertThat(user, notNullValue());
        assertThat(user.principal(), equalTo("user1"));
        assertThat(user.roles(), notNullValue());
        assertThat(user.roles().length, equalTo(2));
        assertThat(user.roles(), arrayContaining("role1", "role2"));
    }

    public void testLookupCaching() throws Exception {
        when(userPasswdStore.userExists("user1")).thenReturn(true);
        when(userRolesStore.roles("user1")).thenReturn(new String[] { "role1", "role2" });
        RealmConfig config = getRealmConfig(globalSettings);
        FileRealm realm = new FileRealm(config, userPasswdStore, userRolesStore, threadPool);

        PlainActionFuture<User> future = new PlainActionFuture<>();
        realm.lookupUser("user1", future);
        User user = future.actionGet();
        future = new PlainActionFuture<>();
        realm.lookupUser("user1", future);
        User user1 = future.actionGet();
        assertThat(user, sameInstance(user1));
        verify(userPasswdStore).userExists("user1");
        verify(userRolesStore).roles("user1");
    }

    public void testLookupCachingWithRefresh() throws Exception {
        RealmConfig config = getRealmConfig(globalSettings);
        userPasswdStore = spy(new UserPasswdStore(config));
        userRolesStore = spy(new UserRolesStore(config));
        doReturn(true).when(userPasswdStore).userExists("user1");
        doReturn(new String[] { "role1", "role2" }).when(userRolesStore).roles("user1");
        FileRealm realm = new FileRealm(config, userPasswdStore, userRolesStore, threadPool);
        PlainActionFuture<User> future = new PlainActionFuture<>();
        realm.lookupUser("user1", future);
        User user1 = future.actionGet();
        future = new PlainActionFuture<>();
        realm.lookupUser("user1", future);
        User user2 = future.actionGet();
        assertThat(user1, sameInstance(user2));

        userPasswdStore.notifyRefresh();

        future = new PlainActionFuture<>();
        realm.lookupUser("user1", future);
        User user3 = future.actionGet();
        assertThat(user2, not(sameInstance(user3)));
        future = new PlainActionFuture<>();
        realm.lookupUser("user1", future);
        User user4 = future.actionGet();
        assertThat(user3, sameInstance(user4));

        userRolesStore.notifyRefresh();

        future = new PlainActionFuture<>();
        realm.lookupUser("user1", future);
        User user5 = future.actionGet();
        assertThat(user4, not(sameInstance(user5)));
        future = new PlainActionFuture<>();
        realm.lookupUser("user1", future);
        User user6 = future.actionGet();
        assertThat(user5, sameInstance(user6));
    }

    public void testUsageStats() throws Exception {
        final int userCount = randomIntBetween(0, 1000);
        when(userPasswdStore.usersCount()).thenReturn(userCount);

        final int order = randomIntBetween(0, 10);
        Settings settings = Settings.builder()
            .put(globalSettings)
            .put(RealmSettings.realmSettingPrefix(REALM_IDENTIFIER) + "order", order)
            .build();

        RealmConfig config = getRealmConfig(settings);
        FileRealm realm = new FileRealm(config, userPasswdStore, userRolesStore, threadPool);

        PlainActionFuture<Map<String, Object>> future = new PlainActionFuture<>();
        realm.usageStats(future);
        Map<String, Object> usage = future.get();
        assertThat(usage, is(notNullValue()));
        assertThat(usage, hasEntry("name", REALM_IDENTIFIER.getName()));
        assertThat(usage, hasEntry("order", order));
        assertThat(usage, hasEntry("size", userCount));
    }

    static class UserPasswdStore extends FileUserPasswdStore {
        UserPasswdStore(RealmConfig config) {
            super(config, mock(ResourceWatcherService.class));
        }
    }

    static class UserRolesStore extends FileUserRolesStore {
        UserRolesStore(RealmConfig config) {
            super(config, mock(ResourceWatcherService.class));
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> Supplier<T> anySupplier() {
        return any(Supplier.class);
    }

}
