/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore.ReservedUserInfo;
import org.elasticsearch.xpack.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.user.ElasticUser;
import org.elasticsearch.xpack.security.user.KibanaUser;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
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

    private static final SecuredString DEFAULT_PASSWORD = new SecuredString("changeme".toCharArray());
    private NativeUsersStore usersStore;

    @Before
    public void setupMocks() {
        usersStore = mock(NativeUsersStore.class);
        when(usersStore.started()).thenReturn(true);
        mockGetAllReservedUserInfo(usersStore, Collections.emptyMap());
    }

    public void testUserStoreNotStarted() {
        when(usersStore.started()).thenReturn(false);
        final ReservedRealm reservedRealm =
                new ReservedRealm(mock(Environment.class), Settings.EMPTY, usersStore, new AnonymousUser(Settings.EMPTY));
        final String principal = randomFrom(ElasticUser.NAME, KibanaUser.NAME);

        ElasticsearchSecurityException expected = expectThrows(ElasticsearchSecurityException.class,
                () -> reservedRealm.doAuthenticate(new UsernamePasswordToken(principal, DEFAULT_PASSWORD)));
        assertThat(expected.getMessage(), containsString("failed to authenticate user [" + principal));
        verify(usersStore).started();
        verifyNoMoreInteractions(usersStore);
    }

    public void testDefaultPasswordAuthentication() throws Throwable {
        final boolean securityIndexExists = randomBoolean();
        if (securityIndexExists) {
            when(usersStore.securityIndexExists()).thenReturn(true);
        }
        final ReservedRealm reservedRealm =
                new ReservedRealm(mock(Environment.class), Settings.EMPTY, usersStore, new AnonymousUser(Settings.EMPTY));
        final User expected = randomFrom(new ElasticUser(true), new KibanaUser(true));
        final String principal = expected.principal();

        final User authenticated = reservedRealm.doAuthenticate(new UsernamePasswordToken(principal, DEFAULT_PASSWORD));
        assertEquals(expected, authenticated);
        verify(usersStore).started();
        verify(usersStore).securityIndexExists();
        if (securityIndexExists) {
            verify(usersStore).getReservedUserInfo(principal);
        }
        verifyNoMoreInteractions(usersStore);
    }

    public void testAuthenticationDisabled() throws Throwable {
        Settings settings = Settings.builder().put(XPackSettings.RESERVED_REALM_ENABLED_SETTING.getKey(), false).build();
        final boolean securityIndexExists = randomBoolean();
        if (securityIndexExists) {
            when(usersStore.securityIndexExists()).thenReturn(true);
        }
        final ReservedRealm reservedRealm = new ReservedRealm(mock(Environment.class), settings, usersStore, new AnonymousUser(settings));
        final User expected = randomFrom(new ElasticUser(true), new KibanaUser(true));
        final String principal = expected.principal();

        final User authenticated = reservedRealm.doAuthenticate(new UsernamePasswordToken(principal, DEFAULT_PASSWORD));
        assertNull(authenticated);
        verifyZeroInteractions(usersStore);
    }

    public void testAuthenticationWithStoredPassword() throws Throwable {
        final ReservedRealm reservedRealm =
                new ReservedRealm(mock(Environment.class), Settings.EMPTY, usersStore, new AnonymousUser(Settings.EMPTY));
        final User expectedUser = randomFrom(new ElasticUser(true), new KibanaUser(true));
        final String principal = expectedUser.principal();
        final SecuredString newPassword = new SecuredString("foobar".toCharArray());
        when(usersStore.securityIndexExists()).thenReturn(true);
        when(usersStore.getReservedUserInfo(principal)).thenReturn(new ReservedUserInfo(Hasher.BCRYPT.hash(newPassword), true));

        // test default password
        ElasticsearchSecurityException expected = expectThrows(ElasticsearchSecurityException.class,
                () -> reservedRealm.doAuthenticate(new UsernamePasswordToken(principal, DEFAULT_PASSWORD)));
        assertThat(expected.getMessage(), containsString("failed to authenticate user [" + principal));

        // the realm assumes it owns the hashed password so it fills it with 0's
        when(usersStore.getReservedUserInfo(principal)).thenReturn(new ReservedUserInfo(Hasher.BCRYPT.hash(newPassword), true));

        // test new password
        final User authenticated = reservedRealm.doAuthenticate(new UsernamePasswordToken(principal, newPassword));
        assertEquals(expectedUser, authenticated);
        verify(usersStore, times(2)).started();
        verify(usersStore, times(2)).securityIndexExists();
        verify(usersStore, times(2)).getReservedUserInfo(principal);
        verifyNoMoreInteractions(usersStore);
    }

    public void testLookup() throws Exception {
        final ReservedRealm reservedRealm =
                new ReservedRealm(mock(Environment.class), Settings.EMPTY, usersStore, new AnonymousUser(Settings.EMPTY));
        final User expectedUser = randomFrom(new ElasticUser(true), new KibanaUser(true));
        final String principal = expectedUser.principal();

        final User user = reservedRealm.doLookupUser(principal);
        assertEquals(expectedUser, user);
        verify(usersStore).started();
        verify(usersStore).securityIndexExists();

        final User doesntExist = reservedRealm.doLookupUser("foobar");
        assertThat(doesntExist, nullValue());
        verifyNoMoreInteractions(usersStore);
    }

    public void testLookupDisabled() throws Exception {
        Settings settings = Settings.builder().put(XPackSettings.RESERVED_REALM_ENABLED_SETTING.getKey(), false).build();
        final ReservedRealm reservedRealm =
                new ReservedRealm(mock(Environment.class), settings, usersStore, new AnonymousUser(settings));
        final User expectedUser = randomFrom(new ElasticUser(true), new KibanaUser(true));
        final String principal = expectedUser.principal();

        final User user = reservedRealm.doLookupUser(principal);
        assertNull(user);
        verifyZeroInteractions(usersStore);
    }

    public void testLookupThrows() throws Exception {
        final ReservedRealm reservedRealm =
                new ReservedRealm(mock(Environment.class), Settings.EMPTY, usersStore, new AnonymousUser(Settings.EMPTY));
        final User expectedUser = randomFrom(new ElasticUser(true), new KibanaUser(true));
        final String principal = expectedUser.principal();
        when(usersStore.securityIndexExists()).thenReturn(true);
        final RuntimeException e = new RuntimeException("store threw");
        when(usersStore.getReservedUserInfo(principal)).thenThrow(e);

        ElasticsearchSecurityException securityException =
                expectThrows(ElasticsearchSecurityException.class, () -> reservedRealm.lookupUser(principal));
        assertThat(securityException.getMessage(), containsString("failed to lookup"));

        verify(usersStore).started();
        verify(usersStore).securityIndexExists();
        verify(usersStore).getReservedUserInfo(principal);
        verifyNoMoreInteractions(usersStore);
    }

    public void testIsReserved() {
        final User expectedUser = randomFrom(new ElasticUser(true), new KibanaUser(true));
        final String principal = expectedUser.principal();
        assertThat(ReservedRealm.isReserved(principal, Settings.EMPTY), is(true));

        final String notExpected = randomFrom("foobar", "", randomAsciiOfLengthBetween(1, 30));
        assertThat(ReservedRealm.isReserved(notExpected, Settings.EMPTY), is(false));
    }

    public void testIsReservedDisabled() {
        Settings settings = Settings.builder().put(XPackSettings.RESERVED_REALM_ENABLED_SETTING.getKey(), false).build();
        final User expectedUser = randomFrom(new ElasticUser(true), new KibanaUser(true));
        final String principal = expectedUser.principal();
        assertThat(ReservedRealm.isReserved(principal, settings), is(false));

        final String notExpected = randomFrom("foobar", "", randomAsciiOfLengthBetween(1, 30));
        assertThat(ReservedRealm.isReserved(notExpected, settings), is(false));
    }

    public void testGetUsers() {
        final ReservedRealm reservedRealm =
                new ReservedRealm(mock(Environment.class), Settings.EMPTY, usersStore, new AnonymousUser(Settings.EMPTY));
        PlainActionFuture<Collection<User>> userFuture = new PlainActionFuture<>();
        reservedRealm.users(userFuture);
        assertThat(userFuture.actionGet(), containsInAnyOrder(new ElasticUser(true), new KibanaUser(true)));
    }

    public void testGetUsersDisabled() {
        final boolean anonymousEnabled = randomBoolean();
        Settings settings = Settings.builder()
                .put(XPackSettings.RESERVED_REALM_ENABLED_SETTING.getKey(), false)
                .put(AnonymousUser.ROLES_SETTING.getKey(), anonymousEnabled ? "user" : "")
                .build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        final ReservedRealm reservedRealm = new ReservedRealm(mock(Environment.class), settings, usersStore, anonymousUser);
        PlainActionFuture<Collection<User>> userFuture = new PlainActionFuture<>();
        reservedRealm.users(userFuture);
        if (anonymousEnabled) {
            assertThat(userFuture.actionGet(), contains(anonymousUser));
        } else {
            assertThat(userFuture.actionGet(), empty());
        }
    }

    public void testFailedAuthentication() {
        final ReservedRealm reservedRealm =
                new ReservedRealm(mock(Environment.class), Settings.EMPTY, usersStore, new AnonymousUser(Settings.EMPTY));
        // maybe cache a successful auth
        if (randomBoolean()) {
            User user = reservedRealm.authenticate(
                    new UsernamePasswordToken(ElasticUser.NAME, new SecuredString("changeme".toCharArray())));
            assertEquals(new ElasticUser(true), user);
        }

        try {
            reservedRealm.authenticate(new UsernamePasswordToken(ElasticUser.NAME, new SecuredString("foobar".toCharArray())));
            fail("authentication should throw an exception otherwise we may allow others to impersonate reserved users...");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.getMessage(), containsString("failed to authenticate"));
        }
    }

    /*
     * NativeUserStore#getAllReservedUserInfo is pkg private we can't mock it otherwise
     */
    public static void mockGetAllReservedUserInfo(NativeUsersStore usersStore, Map<String, ReservedUserInfo> collection) {
        doAnswer((i) -> {
            ((ActionListener) i.getArguments()[0]).onResponse(collection);
            return null;
        }).when(usersStore).getAllReservedUserInfo(any(ActionListener.class));
    }
}
