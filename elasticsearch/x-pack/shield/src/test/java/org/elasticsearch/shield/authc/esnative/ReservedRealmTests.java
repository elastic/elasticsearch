/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.esnative;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.authc.esnative.NativeUsersStore.ChangeListener;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.user.AnonymousUser;
import org.elasticsearch.shield.user.KibanaUser;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.user.XPackUser;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link ReservedRealm}
 */
public class ReservedRealmTests extends ESTestCase {

    private static final SecuredString DEFAULT_PASSWORD = new SecuredString("changeme".toCharArray());
    private NativeUsersStore usersStore;

    @Before
    public void setupMocks() {
        AnonymousUser.initialize(Settings.EMPTY);
        usersStore = mock(NativeUsersStore.class);
        when(usersStore.started()).thenReturn(true);
    }

    public void testUserStoreNotStarted() {
        when(usersStore.started()).thenReturn(false);
        final ReservedRealm reservedRealm = new ReservedRealm(mock(Environment.class), Settings.EMPTY, usersStore);
        final String principal = randomFrom(XPackUser.NAME, KibanaUser.NAME);

        ElasticsearchSecurityException expected = expectThrows(ElasticsearchSecurityException.class,
                () -> reservedRealm.doAuthenticate(new UsernamePasswordToken(principal, DEFAULT_PASSWORD)));
        assertThat(expected.getMessage(), containsString("failed to authenticate user [" + principal));
        verify(usersStore).addListener(any(ChangeListener.class));
        verify(usersStore).started();
        verifyNoMoreInteractions(usersStore);
    }

    public void testDefaultPasswordAuthentication() throws Throwable {
        final boolean shieldIndexExists = randomBoolean();
        if (shieldIndexExists) {
            when(usersStore.shieldIndexExists()).thenReturn(true);
        }
        final ReservedRealm reservedRealm = new ReservedRealm(mock(Environment.class), Settings.EMPTY, usersStore);
        final User expected = randomFrom((User) XPackUser.INSTANCE, (User) KibanaUser.INSTANCE);
        final String principal = expected.principal();

        final User authenticated = reservedRealm.doAuthenticate(new UsernamePasswordToken(principal, DEFAULT_PASSWORD));
        assertThat(authenticated, sameInstance(expected));
        verify(usersStore).addListener(any(ChangeListener.class));
        verify(usersStore).started();
        verify(usersStore).shieldIndexExists();
        if (shieldIndexExists) {
            verify(usersStore).reservedUserPassword(principal);
        }
        verifyNoMoreInteractions(usersStore);
    }

    public void testAuthenticationWithStoredPassword() throws Throwable {
        final ReservedRealm reservedRealm = new ReservedRealm(mock(Environment.class), Settings.EMPTY, usersStore);
        final User expectedUser = randomFrom((User) XPackUser.INSTANCE, (User) KibanaUser.INSTANCE);
        final String principal = expectedUser.principal();
        final SecuredString newPassword = new SecuredString("foobar".toCharArray());
        when(usersStore.shieldIndexExists()).thenReturn(true);
        when(usersStore.reservedUserPassword(principal)).thenReturn(Hasher.BCRYPT.hash(newPassword));

        // test default password
        ElasticsearchSecurityException expected = expectThrows(ElasticsearchSecurityException.class,
                () -> reservedRealm.doAuthenticate(new UsernamePasswordToken(principal, DEFAULT_PASSWORD)));
        assertThat(expected.getMessage(), containsString("failed to authenticate user [" + principal));

        // the realm assumes it owns the hashed password so it fills it with 0's
        when(usersStore.reservedUserPassword(principal)).thenReturn(Hasher.BCRYPT.hash(newPassword));

        // test new password
        final User authenticated = reservedRealm.doAuthenticate(new UsernamePasswordToken(principal, newPassword));
        assertThat(authenticated, sameInstance(expectedUser));
        verify(usersStore).addListener(any(ChangeListener.class));
        verify(usersStore, times(2)).started();
        verify(usersStore, times(2)).shieldIndexExists();
        verify(usersStore, times(2)).reservedUserPassword(principal);
        verifyNoMoreInteractions(usersStore);
    }

    public void testLookup() {
        final ReservedRealm reservedRealm = new ReservedRealm(mock(Environment.class), Settings.EMPTY, usersStore);
        final User expectedUser = randomFrom((User) XPackUser.INSTANCE, (User) KibanaUser.INSTANCE);
        final String principal = expectedUser.principal();

        final User user = reservedRealm.doLookupUser(principal);
        assertThat(user, sameInstance(expectedUser));
        verify(usersStore).addListener(any(ChangeListener.class));
        verifyNoMoreInteractions(usersStore);

        final User doesntExist = reservedRealm.doLookupUser("foobar");
        assertThat(doesntExist, nullValue());
    }

    public void testHelperMethods() {
        final User expectedUser = randomFrom((User) XPackUser.INSTANCE, (User) KibanaUser.INSTANCE);
        final String principal = expectedUser.principal();
        assertThat(ReservedRealm.isReserved(principal), is(true));
        assertThat(ReservedRealm.getUser(principal), sameInstance(expectedUser));

        final String notExpected = randomFrom("foobar", "", randomAsciiOfLengthBetween(1, 30));
        assertThat(ReservedRealm.isReserved(notExpected), is(false));
        assertThat(ReservedRealm.getUser(notExpected), nullValue());

        assertThat(ReservedRealm.users(), containsInAnyOrder((User) XPackUser.INSTANCE, KibanaUser.INSTANCE));
    }

    public void testFailedAuthentication() {
        final ReservedRealm reservedRealm = new ReservedRealm(mock(Environment.class), Settings.EMPTY, usersStore);
        // maybe cache a successful auth
        if (randomBoolean()) {
            User user = reservedRealm.authenticate(new UsernamePasswordToken(XPackUser.NAME, new SecuredString("changeme".toCharArray())));
            assertThat(user, sameInstance(XPackUser.INSTANCE));
        }

        try {
            reservedRealm.authenticate(new UsernamePasswordToken(XPackUser.NAME, new SecuredString("foobar".toCharArray())));
            fail("authentication should throw an exception otherwise we may allow others to impersonate reserved users...");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.getMessage(), containsString("failed to authenticate"));
        }
    }
}
