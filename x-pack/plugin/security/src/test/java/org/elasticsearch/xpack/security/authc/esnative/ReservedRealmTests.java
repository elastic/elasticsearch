/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.esnative.ClientReservedRealm;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.APMSystemUser;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.BeatsSystemUser;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.KibanaSystemUser;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.elasticsearch.xpack.core.security.user.LogstashSystemUser;
import org.elasticsearch.xpack.core.security.user.RemoteMonitoringUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.UsernamesField;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore.ReservedUserInfo;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link ReservedRealm}
 */
public class ReservedRealmTests extends ESTestCase {

    private static final SecureString EMPTY_PASSWORD = new SecureString("".toCharArray());
    private NativeUsersStore usersStore;
    private SecurityIndexManager securityIndex;
    private ThreadPool threadPool;

    @Before
    public void setupMocks() throws Exception {
        usersStore = mock(NativeUsersStore.class);
        securityIndex = mock(SecurityIndexManager.class);
        when(securityIndex.isAvailable()).thenReturn(true);
        mockGetAllReservedUserInfo(usersStore, Collections.emptyMap());
        threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
    }

    public void testInvalidHashingAlgorithmFails() {
        final String invalidAlgoId = randomFrom("sha1", "md5", "noop");
        final Settings invalidSettings = Settings.builder().put("xpack.security.authc.password_hashing.algorithm", invalidAlgoId).build();
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new ReservedRealm(mock(Environment.class),
            invalidSettings, usersStore, new AnonymousUser(Settings.EMPTY), securityIndex, threadPool));
        assertThat(exception.getMessage(), containsString(invalidAlgoId));
        assertThat(exception.getMessage(), containsString("Invalid algorithm"));
    }

    public void testReservedUserEmptyPasswordAuthenticationFails() throws Throwable {
        final String principal = randomFrom(UsernamesField.ELASTIC_NAME, UsernamesField.KIBANA_NAME, UsernamesField.LOGSTASH_NAME,
            UsernamesField.BEATS_NAME);

        final ReservedRealm reservedRealm = new ReservedRealm(mock(Environment.class), Settings.EMPTY, usersStore,
            new AnonymousUser(Settings.EMPTY), securityIndex, threadPool);

        PlainActionFuture<AuthenticationResult> listener = new PlainActionFuture<>();

        reservedRealm.doAuthenticate(new UsernamePasswordToken(principal, EMPTY_PASSWORD), listener);
        assertFailedAuthentication(listener, principal);
    }

    public void testAuthenticationDisabled() throws Throwable {
        Settings settings = Settings.builder().put(XPackSettings.RESERVED_REALM_ENABLED_SETTING.getKey(), false).build();
        final boolean securityIndexExists = randomBoolean();
        if (securityIndexExists) {
            when(securityIndex.indexExists()).thenReturn(true);
        }
        final ReservedRealm reservedRealm =
            new ReservedRealm(mock(Environment.class), settings, usersStore,
                new AnonymousUser(settings), securityIndex, threadPool);
        final User expected = randomReservedUser(true);
        final String principal = expected.principal();

        PlainActionFuture<AuthenticationResult> listener = new PlainActionFuture<>();
        reservedRealm.doAuthenticate(new UsernamePasswordToken(principal, EMPTY_PASSWORD), listener);
        final AuthenticationResult result = listener.actionGet();
        assertThat(result.getStatus(), is(AuthenticationResult.Status.CONTINUE));
        assertNull(result.getUser());
        verifyZeroInteractions(usersStore);
    }

    public void testAuthenticationEnabledUserWithStoredPassword() throws Throwable {
        verifySuccessfulAuthentication(true);
    }

    public void testAuthenticationDisabledUserWithStoredPassword() throws Throwable {
        verifySuccessfulAuthentication(false);
    }

    private void verifySuccessfulAuthentication(boolean enabled) throws Exception {
        final ReservedRealm reservedRealm = new ReservedRealm(mock(Environment.class), Settings.EMPTY, usersStore,
            new AnonymousUser(Settings.EMPTY), securityIndex, threadPool);
        final User expectedUser = randomReservedUser(enabled);
        final String principal = expectedUser.principal();
        final SecureString newPassword = new SecureString("foobar".toCharArray());
        // Mocked users store is initiated with default hashing algorithm
        final Hasher hasher = Hasher.resolve("bcrypt");
        when(securityIndex.indexExists()).thenReturn(true);
        doAnswer(getAnswer(enabled, newPassword, hasher)).when(usersStore).getReservedUserInfo(eq(principal), anyActionListener());

        // test empty password
        final PlainActionFuture<AuthenticationResult> listener = new PlainActionFuture<>();
        reservedRealm.doAuthenticate(new UsernamePasswordToken(principal, EMPTY_PASSWORD), listener);
        assertFailedAuthentication(listener, expectedUser.principal());

        // the realm assumes it owns the hashed password so it fills it with 0's
        doAnswer(getAnswer(true, newPassword, hasher)).when(usersStore).getReservedUserInfo(eq(principal), anyActionListener());

        // test new password
        final PlainActionFuture<AuthenticationResult> authListener = new PlainActionFuture<>();
        reservedRealm.doAuthenticate(new UsernamePasswordToken(principal, newPassword), authListener);
        final User authenticated = authListener.actionGet().getUser();
        assertEquals(expectedUser, authenticated);
        assertThat(expectedUser.enabled(), is(enabled));

        verify(securityIndex, times(2)).indexExists();
        verify(usersStore, times(2)).getReservedUserInfo(eq(principal), anyActionListener());
        verifyNoMoreInteractions(usersStore);

        if (new KibanaUser(enabled).equals(expectedUser)) {
            assertWarnings("The user [kibana] is deprecated and will be removed in a future version of Elasticsearch. " +
                            "Please use the [kibana_system] user instead.");
        }
    }

    @SuppressWarnings("unchecked")
    private Answer<Class<Void>> getAnswer(boolean enabled, SecureString newPassword, Hasher hasher) {
        return (i) -> {
            ActionListener<ReservedUserInfo> callback = (ActionListener<ReservedUserInfo>) i.getArguments()[1];
            callback.onResponse(new ReservedUserInfo(hasher.hash(newPassword), enabled));
            return null;
        };
    }

    public void testLookup() throws Exception {
        final ReservedRealm reservedRealm =
            new ReservedRealm(mock(Environment.class), Settings.EMPTY, usersStore,
                new AnonymousUser(Settings.EMPTY), securityIndex, threadPool);
        final User expectedUser = randomReservedUser(true);
        final String principal = expectedUser.principal();

        PlainActionFuture<User> listener = new PlainActionFuture<>();
        reservedRealm.doLookupUser(principal, listener);
        final User user = listener.actionGet();
        assertEquals(expectedUser, user);
        verify(securityIndex).indexExists();

        PlainActionFuture<User> future = new PlainActionFuture<>();
        reservedRealm.doLookupUser("foobar", assertListenerIsOnlyCalledOnce(future));
        final User doesntExist = future.actionGet();
        assertThat(doesntExist, nullValue());
        verifyNoMoreInteractions(usersStore);
    }

    public void testLookupDisabled() throws Exception {
        Settings settings = Settings.builder().put(XPackSettings.RESERVED_REALM_ENABLED_SETTING.getKey(), false).build();
        final ReservedRealm reservedRealm =
            new ReservedRealm(mock(Environment.class), settings, usersStore, new AnonymousUser(settings),
                securityIndex, threadPool);
        final User expectedUser = randomReservedUser(true);
        final String principal = expectedUser.principal();

        PlainActionFuture<User> listener = new PlainActionFuture<>();
        reservedRealm.doLookupUser(principal, assertListenerIsOnlyCalledOnce(listener));
        final User user = listener.actionGet();
        assertNull(user);
        verifyZeroInteractions(usersStore);
    }


    public void testLookupDisabledAnonymous() throws Exception {
        Settings settings = Settings.builder()
            .put(XPackSettings.RESERVED_REALM_ENABLED_SETTING.getKey(), false)
            .put(AnonymousUser.ROLES_SETTING.getKey(), "anonymous")
            .build();
        final ReservedRealm reservedRealm =
            new ReservedRealm(mock(Environment.class), settings, usersStore, new AnonymousUser(settings),
                securityIndex, threadPool);
        final User expectedUser = new AnonymousUser(settings);
        final String principal = expectedUser.principal();

        PlainActionFuture<User> listener = new PlainActionFuture<>();
        reservedRealm.doLookupUser(principal, assertListenerIsOnlyCalledOnce(listener));
        assertThat(listener.actionGet(), equalTo(expectedUser));
    }

    public void testLookupThrows() throws Exception {
        final ReservedRealm reservedRealm =
            new ReservedRealm(mock(Environment.class), Settings.EMPTY, usersStore,
                new AnonymousUser(Settings.EMPTY), securityIndex, threadPool);
        final User expectedUser = randomReservedUser(true);
        final String principal = expectedUser.principal();
        when(securityIndex.indexExists()).thenReturn(true);
        final RuntimeException e = new RuntimeException("store threw");
        doAnswer((i) -> {
            ActionListener<?> callback = (ActionListener<?>) i.getArguments()[1];
            callback.onFailure(e);
            return null;
        }).when(usersStore).getReservedUserInfo(eq(principal), anyActionListener());

        PlainActionFuture<User> future = new PlainActionFuture<>();
        reservedRealm.lookupUser(principal, future);
        ElasticsearchSecurityException securityException = expectThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(securityException.getMessage(), containsString("failed to lookup"));

        verify(securityIndex).indexExists();
        verify(usersStore).getReservedUserInfo(eq(principal), anyActionListener());

        verifyNoMoreInteractions(usersStore);
    }

    public void testIsReserved() {
        final User expectedUser = randomReservedUser(true);
        final String principal = expectedUser.principal();
        assertThat(ClientReservedRealm.isReserved(principal, Settings.EMPTY), is(true));

        final String notExpected = randomFrom("foobar", "", randomAlphaOfLengthBetween(1, 30));
        assertThat(ClientReservedRealm.isReserved(notExpected, Settings.EMPTY), is(false));
    }

    public void testIsReservedDisabled() {
        Settings settings = Settings.builder().put(XPackSettings.RESERVED_REALM_ENABLED_SETTING.getKey(), false).build();
        final User expectedUser = randomReservedUser(true);
        final String principal = expectedUser.principal();
        assertThat(ClientReservedRealm.isReserved(principal, settings), is(false));

        final String notExpected = randomFrom("foobar", "", randomAlphaOfLengthBetween(1, 30));
        assertThat(ClientReservedRealm.isReserved(notExpected, settings), is(false));
    }

    public void testGetUsers() {
        final ReservedRealm reservedRealm = new ReservedRealm(mock(Environment.class), Settings.EMPTY, usersStore,
            new AnonymousUser(Settings.EMPTY), securityIndex, threadPool);
        PlainActionFuture<Collection<User>> userFuture = new PlainActionFuture<>();
        reservedRealm.users(userFuture);
        assertThat(userFuture.actionGet(),
            containsInAnyOrder(new ElasticUser(true), new KibanaUser(true), new KibanaSystemUser(true),
                new LogstashSystemUser(true), new BeatsSystemUser(true), new APMSystemUser(true), new RemoteMonitoringUser(true)));
    }

    public void testGetUsersDisabled() {
        final boolean anonymousEnabled = randomBoolean();
        Settings settings = Settings.builder()
            .put(XPackSettings.RESERVED_REALM_ENABLED_SETTING.getKey(), false)
            .put(AnonymousUser.ROLES_SETTING.getKey(), anonymousEnabled ? "user" : "")
            .build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        final ReservedRealm reservedRealm = new ReservedRealm(mock(Environment.class), settings, usersStore, anonymousUser,
            securityIndex, threadPool);
        PlainActionFuture<Collection<User>> userFuture = new PlainActionFuture<>();
        reservedRealm.users(userFuture);
        if (anonymousEnabled) {
            assertThat(userFuture.actionGet(), contains(anonymousUser));
        } else {
            assertThat(userFuture.actionGet(), empty());
        }
    }

    public void testFailedAuthentication() throws Exception {
        when(securityIndex.indexExists()).thenReturn(true);
        SecureString password = new SecureString("password".toCharArray());
        // Mocked users store is initiated with default hashing algorithm
        final Hasher hasher = Hasher.resolve("bcrypt");
        char[] hash = hasher.hash(password);
        ReservedUserInfo userInfo = new ReservedUserInfo(hash, true);
        mockGetAllReservedUserInfo(usersStore, Collections.singletonMap("elastic", userInfo));
        final ReservedRealm reservedRealm = new ReservedRealm(mock(Environment.class), Settings.EMPTY, usersStore,
            new AnonymousUser(Settings.EMPTY), securityIndex, threadPool);

        if (randomBoolean()) {
            PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();

            reservedRealm.authenticate(new UsernamePasswordToken(ElasticUser.NAME, password), future);
            User user = future.actionGet().getUser();
            assertEquals(new ElasticUser(true), user);
        }

        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        reservedRealm.authenticate(new UsernamePasswordToken(ElasticUser.NAME, new SecureString("foobar".toCharArray())), future);
        assertFailedAuthentication(future, ElasticUser.NAME);
    }

    private void assertFailedAuthentication(PlainActionFuture<AuthenticationResult> future, String principal) throws Exception {
        final AuthenticationResult result = future.get();
        assertThat(result.getStatus(), is(AuthenticationResult.Status.TERMINATE));
        assertThat(result.getMessage(), containsString("failed to authenticate"));
        assertThat(result.getMessage(), containsString(principal));
        // exception must be null for graceful termination
        assertThat(result.getException(), is(nullValue()));
    }

    @SuppressWarnings("unchecked")
    public void testBootstrapElasticPasswordWorksOnceSecurityIndexExists() throws Exception {
        MockSecureSettings mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("bootstrap.password", "foobar");
        Settings settings = Settings.builder().setSecureSettings(mockSecureSettings).build();
        when(securityIndex.indexExists()).thenReturn(true);

        final ReservedRealm reservedRealm = new ReservedRealm(mock(Environment.class), settings, usersStore,
            new AnonymousUser(Settings.EMPTY), securityIndex, threadPool);
        PlainActionFuture<AuthenticationResult> listener = new PlainActionFuture<>();

        doAnswer((i) -> {
            @SuppressWarnings("rawtypes")
            ActionListener callback = (ActionListener) i.getArguments()[1];
            callback.onResponse(null);
            return null;
        }).when(usersStore).getReservedUserInfo(eq("elastic"), anyActionListener());
        reservedRealm.doAuthenticate(new UsernamePasswordToken(new ElasticUser(true).principal(),
                mockSecureSettings.getString("bootstrap.password")),
            listener);
        final AuthenticationResult result = listener.get();
        assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
    }

    public void testBootstrapElasticPasswordFailsOnceElasticUserExists() throws Exception {
        MockSecureSettings mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("bootstrap.password", "foobar");
        Settings settings = Settings.builder().setSecureSettings(mockSecureSettings).build();
        when(securityIndex.indexExists()).thenReturn(true);

        final ReservedRealm reservedRealm = new ReservedRealm(mock(Environment.class), settings, usersStore,
            new AnonymousUser(Settings.EMPTY), securityIndex, threadPool);
        PlainActionFuture<AuthenticationResult> listener = new PlainActionFuture<>();
        SecureString password = new SecureString("password".toCharArray());
        // Mocked users store is initiated with default hashing algorithm
        final Hasher hasher = Hasher.resolve("bcrypt");
        doAnswer((i) -> {
            @SuppressWarnings("unchecked")
            ActionListener<ReservedUserInfo> callback = (ActionListener<ReservedUserInfo>) i.getArguments()[1];
            char[] hash = hasher.hash(password);
            ReservedUserInfo userInfo = new ReservedUserInfo(hash, true);
            callback.onResponse(userInfo);
            return null;
        }).when(usersStore).getReservedUserInfo(eq("elastic"), anyActionListener());
        reservedRealm.doAuthenticate(new UsernamePasswordToken(new ElasticUser(true).principal(),
            mockSecureSettings.getString("bootstrap.password")), listener);
        assertFailedAuthentication(listener, "elastic");
        // now try with the real password
        listener = new PlainActionFuture<>();
        reservedRealm.doAuthenticate(new UsernamePasswordToken(new ElasticUser(true).principal(), password), listener);
        final AuthenticationResult result = listener.get();
        assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
    }

    public void testBootstrapElasticPasswordWorksBeforeSecurityIndexExists() throws ExecutionException, InterruptedException {
        MockSecureSettings mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("bootstrap.password", "foobar");
        Settings settings = Settings.builder().setSecureSettings(mockSecureSettings).build();
        when(securityIndex.indexExists()).thenReturn(false);

        final ReservedRealm reservedRealm = new ReservedRealm(mock(Environment.class), settings, usersStore,
            new AnonymousUser(Settings.EMPTY), securityIndex, threadPool);
        PlainActionFuture<AuthenticationResult> listener = new PlainActionFuture<>();

        reservedRealm.doAuthenticate(new UsernamePasswordToken(new ElasticUser(true).principal(),
                mockSecureSettings.getString("bootstrap.password")),
            listener);
        final AuthenticationResult result = listener.get();
        assertThat(result.getStatus(), is(AuthenticationResult.Status.SUCCESS));
    }

    public void testNonElasticUsersCannotUseBootstrapPasswordWhenSecurityIndexExists() throws Exception {
        final MockSecureSettings mockSecureSettings = new MockSecureSettings();
        final String password = randomAlphaOfLengthBetween(8, 24);
        mockSecureSettings.setString("bootstrap.password", password);
        Settings settings = Settings.builder().setSecureSettings(mockSecureSettings).build();
        when(securityIndex.indexExists()).thenReturn(true);

        final ReservedRealm reservedRealm = new ReservedRealm(mock(Environment.class), settings, usersStore,
            new AnonymousUser(Settings.EMPTY), securityIndex, threadPool);
        PlainActionFuture<AuthenticationResult> listener = new PlainActionFuture<>();

        final String principal = randomFrom(KibanaUser.NAME, KibanaSystemUser.NAME, LogstashSystemUser.NAME, BeatsSystemUser.NAME,
            APMSystemUser.NAME, RemoteMonitoringUser.NAME);
        doAnswer((i) -> {
            ActionListener<?> callback = (ActionListener<?>) i.getArguments()[1];
            callback.onResponse(null);
            return null;
        }).when(usersStore).getReservedUserInfo(eq(principal), anyActionListener());
        reservedRealm.doAuthenticate(new UsernamePasswordToken(principal, mockSecureSettings.getString("bootstrap.password")), listener);
        final AuthenticationResult result = listener.get();
        assertThat(result.getStatus(), is(AuthenticationResult.Status.TERMINATE));
    }

    public void testNonElasticUsersCannotUseBootstrapPasswordWhenSecurityIndexDoesNotExists() throws Exception {
        final MockSecureSettings mockSecureSettings = new MockSecureSettings();
        final String password = randomAlphaOfLengthBetween(8, 24);
        mockSecureSettings.setString("bootstrap.password", password);
        Settings settings = Settings.builder().setSecureSettings(mockSecureSettings).build();
        when(securityIndex.indexExists()).thenReturn(false);

        final ReservedRealm reservedRealm = new ReservedRealm(mock(Environment.class), settings, usersStore,
            new AnonymousUser(Settings.EMPTY), securityIndex, threadPool);
        PlainActionFuture<AuthenticationResult> listener = new PlainActionFuture<>();

        final String principal = randomFrom(KibanaUser.NAME, KibanaSystemUser.NAME, LogstashSystemUser.NAME, BeatsSystemUser.NAME,
            APMSystemUser.NAME, RemoteMonitoringUser.NAME);
        reservedRealm.doAuthenticate(new UsernamePasswordToken(principal, mockSecureSettings.getString("bootstrap.password")), listener);
        final AuthenticationResult result = listener.get();
        assertThat(result.getStatus(), is(AuthenticationResult.Status.TERMINATE));
    }

    private User randomReservedUser(boolean enabled) {
        return randomFrom(new ElasticUser(enabled), new KibanaUser(enabled), new KibanaSystemUser(enabled),
            new LogstashSystemUser(enabled), new BeatsSystemUser(enabled), new APMSystemUser(enabled), new RemoteMonitoringUser(enabled));
    }

    /*
     * NativeUserStore#getAllReservedUserInfo is pkg private we can't mock it otherwise
     */
    @SuppressWarnings("unchecked")
    public static void mockGetAllReservedUserInfo(NativeUsersStore usersStore, Map<String, ReservedUserInfo> collection) {
        doAnswer((i) -> {
            ((ActionListener<Map<String, ReservedUserInfo>>) i.getArguments()[0]).onResponse(collection);
            return null;
        }).when(usersStore).getAllReservedUserInfo(anyActionListener());

        for (Entry<String, ReservedUserInfo> entry : collection.entrySet()) {
            doAnswer((i) -> {
                ((ActionListener<ReservedUserInfo>) i.getArguments()[1]).onResponse(entry.getValue());
                return null;
            }).when(usersStore).getReservedUserInfo(eq(entry.getKey()), anyActionListener());
        }
    }

    private static <T> ActionListener<T> assertListenerIsOnlyCalledOnce(ActionListener<T> delegate) {
        final AtomicInteger callCount = new AtomicInteger(0);
        return ActionListener.runBefore(delegate, () -> {
            if (callCount.incrementAndGet() != 1) {
                fail("Listener was called twice");
            }
        });
    }
}
