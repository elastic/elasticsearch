/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequest;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authc.file.FileRealm;
import org.elasticsearch.xpack.security.authc.pki.PkiRealm;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.test.SecurityIntegTestCase.getFastStoredHashAlgoForTests;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class TransportChangePasswordActionTests extends ESTestCase {

    public void testAnonymousUser() {
        final Hasher hasher = getFastStoredHashAlgoForTests();
        Settings settings = Settings.builder()
            .put(AnonymousUser.ROLES_SETTING.getKey(), "superuser")
            .put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), hasher.name())
            .build();
        AnonymousUser anonymousUser = new AnonymousUser(settings);
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        TransportChangePasswordAction action = new TransportChangePasswordAction(
            settings,
            transportService,
            mock(ActionFilters.class),
            usersStore,
            mockRealms()
        );
        // Request will fail before the request hashing algorithm is checked, but we use the same algorithm as in settings for consistency
        ChangePasswordRequest request = new ChangePasswordRequest();
        request.username(anonymousUser.principal());
        request.passwordHash(hasher.hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<ActionResponse.Empty> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<>() {
            @Override
            public void onResponse(ActionResponse.Empty changePasswordResponse) {
                responseRef.set(changePasswordResponse);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), instanceOf(IllegalArgumentException.class));
        assertThat(throwableRef.get().getMessage(), containsString("is anonymous and cannot be modified"));
        verifyNoMoreInteractions(usersStore);
    }

    public void testFileRealmUser() {
        final Hasher hasher = getFastStoredHashAlgoForTests();
        Settings settings = Settings.builder()
            .put(AnonymousUser.ROLES_SETTING.getKey(), "superuser")
            .put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), hasher.name())
            .put(XPackSettings.RESERVED_REALM_ENABLED_SETTING.getKey(), false) // simulate cloud set up
            .build();
        ElasticUser elasticUser = new ElasticUser(true);
        NativeUsersStore usersStore = mock(NativeUsersStore.class);

        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        TransportChangePasswordAction action = new TransportChangePasswordAction(
            settings,
            transportService,
            mock(ActionFilters.class),
            usersStore,
            mockRealms(mockFileRealm(elasticUser))
        );
        ChangePasswordRequest request = new ChangePasswordRequest();
        request.username(elasticUser.principal());
        request.passwordHash(hasher.hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            @SuppressWarnings("unchecked")
            ActionListener<User> listener = (ActionListener<User>) args[1];
            listener.onResponse(null);
            return null;
        }).when(usersStore).getUser(eq(request.username()), anyActionListener());

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<ActionResponse.Empty> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<>() {
            @Override
            public void onResponse(ActionResponse.Empty changePasswordResponse) {
                responseRef.set(changePasswordResponse);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), instanceOf(ValidationException.class));
        assertThat(
            throwableRef.get().getMessage(),
            containsString("In a cloud deployment, the password can be changed through the " + "cloud console.")
        );
        verify(usersStore, times(0)).changePassword(any(ChangePasswordRequest.class), anyActionListener());
    }

    public void testUserPresentInNonNativeRealms() {
        // place a user 'foo' in both file and pki realms. it will not be found in the native realm.
        final Hasher hasher = getFastStoredHashAlgoForTests();
        Settings settings = Settings.builder()
            .put(AnonymousUser.ROLES_SETTING.getKey(), "superuser")
            .put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), hasher.name())
            .put(XPackSettings.RESERVED_REALM_ENABLED_SETTING.getKey(), true)
            .build();
        User foo = new User("foo", "admin");
        NativeUsersStore usersStore = mock(NativeUsersStore.class);

        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        TransportChangePasswordAction action = new TransportChangePasswordAction(
            settings,
            transportService,
            mock(ActionFilters.class),
            usersStore,
            mockRealms(mockFileRealm(foo), mockPkiRealm(foo))
        );
        ChangePasswordRequest request = new ChangePasswordRequest();
        request.username(foo.principal());
        request.passwordHash(hasher.hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            @SuppressWarnings("unchecked")
            ActionListener<User> listener = (ActionListener<User>) args[1];
            listener.onResponse(null);
            return null;
        }).when(usersStore).getUser(eq(request.username()), anyActionListener());

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<ActionResponse.Empty> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<>() {
            @Override
            public void onResponse(ActionResponse.Empty changePasswordResponse) {
                responseRef.set(changePasswordResponse);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), instanceOf(ValidationException.class));
        assertThat(
            throwableRef.get().getMessage(),
            containsString("user [" + foo.principal() + "] does not belong to the native realm and cannot be managed via this API.")
        );
        verify(usersStore, times(0)).changePassword(any(ChangePasswordRequest.class), anyActionListener());
    }

    public void testValidUser() {
        testValidUser(randomFrom(new ElasticUser(true), new KibanaUser(true), new User("joe")));
    }

    public void testValidUserWithInternalUsername() {
        testValidUser(new User(AuthenticationTestHelper.randomInternalUsername()));
    }

    private void testValidUser(User user) {
        final Hasher hasher = getFastStoredHashAlgoForTests();
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        ChangePasswordRequest request = new ChangePasswordRequest();
        request.username(user.principal());
        request.passwordHash(hasher.hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            @SuppressWarnings("unchecked")
            ActionListener<Void> listener = (ActionListener<Void>) args[1];
            listener.onResponse(null);
            return null;
        }).when(usersStore).changePassword(eq(request), anyActionListener());
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            @SuppressWarnings("unchecked")
            ActionListener<User> listener = (ActionListener<User>) args[1];
            listener.onResponse(user);
            return null;
        }).when(usersStore).getUser(eq(request.username()), anyActionListener());
        Settings passwordHashingSettings = Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), hasher.name()).build();
        TransportChangePasswordAction action = new TransportChangePasswordAction(
            passwordHashingSettings,
            transportService,
            mock(ActionFilters.class),
            usersStore,
            mockRealms()
        );
        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<ActionResponse.Empty> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<>() {
            @Override
            public void onResponse(ActionResponse.Empty changePasswordResponse) {
                responseRef.set(changePasswordResponse);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(notNullValue()));
        assertSame(responseRef.get(), ActionResponse.Empty.INSTANCE);
        assertThat(throwableRef.get(), is(nullValue()));
        verify(usersStore, times(1)).changePassword(eq(request), anyActionListener());
    }

    public void testWithPasswordThatsNotAHash() {
        final User user = randomFrom(new ElasticUser(true), new KibanaUser(true), new User("joe"));
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        ChangePasswordRequest request = new ChangePasswordRequest();
        request.username(user.principal());
        request.passwordHash(randomAlphaOfLengthBetween(14, 20).toCharArray());
        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<ActionResponse.Empty> responseRef = new AtomicReference<>();
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        final String systemHash = randomFrom("pbkdf2_50000", "pbkdf2_100000", "bcrypt11", "bcrypt8", "bcrypt");
        Settings passwordHashingSettings = Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), systemHash).build();
        TransportChangePasswordAction action = new TransportChangePasswordAction(
            passwordHashingSettings,
            transportService,
            mock(ActionFilters.class),
            usersStore,
            mockRealms()
        );
        action.doExecute(mock(Task.class), request, new ActionListener<>() {
            @Override
            public void onResponse(ActionResponse.Empty changePasswordResponse) {
                responseRef.set(changePasswordResponse);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), instanceOf(IllegalArgumentException.class));
        assertThat(
            throwableRef.get().getMessage(),
            containsString("The provided password hash is not a hash or it could not be resolved to a supported hash algorithm.")
        );
        verifyNoMoreInteractions(usersStore);
    }

    public void testWithDifferentPasswordHashingAlgorithm() {
        final User user = randomFrom(new ElasticUser(true), new KibanaUser(true), new User("joe"));
        final Hasher hasher = getFastStoredHashAlgoForTests();
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        ChangePasswordRequest request = new ChangePasswordRequest();
        request.username(user.principal());
        request.passwordHash(hasher.hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            @SuppressWarnings("unchecked")
            ActionListener<Void> listener = (ActionListener<Void>) args[1];
            listener.onResponse(null);
            return null;
        }).when(usersStore).changePassword(eq(request), anyActionListener());
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            @SuppressWarnings("unchecked")
            ActionListener<User> listener = (ActionListener<User>) args[1];
            listener.onResponse(user);
            return null;
        }).when(usersStore).getUser(eq(request.username()), anyActionListener());
        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<ActionResponse.Empty> responseRef = new AtomicReference<>();
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        final String systemHash = randomValueOtherThan(
            hasher.name().toLowerCase(Locale.ROOT),
            () -> randomFrom("pbkdf2_50000", "pbkdf2_100000", "bcrypt11", "bcrypt8", "bcrypt")
        );
        Settings passwordHashingSettings = Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), systemHash).build();
        TransportChangePasswordAction action = new TransportChangePasswordAction(
            passwordHashingSettings,
            transportService,
            mock(ActionFilters.class),
            usersStore,
            mockRealms()
        );
        action.doExecute(mock(Task.class), request, new ActionListener<>() {
            @Override
            public void onResponse(ActionResponse.Empty changePasswordResponse) {
                responseRef.set(changePasswordResponse);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(notNullValue()));
        assertSame(responseRef.get(), ActionResponse.Empty.INSTANCE);
        assertThat(throwableRef.get(), is(nullValue()));
        verify(usersStore, times(1)).changePassword(eq(request), anyActionListener());
    }

    public void testException() {
        final Hasher hasher = getFastStoredHashAlgoForTests();
        final User user = randomFrom(new ElasticUser(true), new KibanaUser(true), new User("joe"));
        NativeUsersStore usersStore = mock(NativeUsersStore.class);
        ChangePasswordRequest request = new ChangePasswordRequest();
        request.username(user.principal());
        request.passwordHash(hasher.hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
        final Exception e = randomFrom(new ElasticsearchSecurityException(""), new IllegalStateException(), new RuntimeException());
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            @SuppressWarnings("unchecked")
            ActionListener<Void> listener = (ActionListener<Void>) args[1];
            listener.onFailure(e);
            return null;
        }).when(usersStore).changePassword(eq(request), anyActionListener());
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            @SuppressWarnings("unchecked")
            ActionListener<User> listener = (ActionListener<User>) args[1];
            listener.onResponse(user);
            return null;
        }).when(usersStore).getUser(eq(request.username()), anyActionListener());
        TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            mock(ThreadPool.class),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );
        Settings passwordHashingSettings = Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), hasher.name()).build();
        TransportChangePasswordAction action = new TransportChangePasswordAction(
            passwordHashingSettings,
            transportService,
            mock(ActionFilters.class),
            usersStore,
            mockRealms()
        );
        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        final AtomicReference<ActionResponse.Empty> responseRef = new AtomicReference<>();
        action.doExecute(mock(Task.class), request, new ActionListener<>() {
            @Override
            public void onResponse(ActionResponse.Empty changePasswordResponse) {
                responseRef.set(changePasswordResponse);
            }

            @Override
            public void onFailure(Exception e) {
                throwableRef.set(e);
            }
        });

        assertThat(responseRef.get(), is(nullValue()));
        assertThat(throwableRef.get(), is(notNullValue()));
        assertThat(throwableRef.get(), sameInstance(e));
        verify(usersStore, times(1)).changePassword(eq(request), anyActionListener());
    }

    private static FileRealm mockFileRealm(User fileRealmUser) {

        FileRealm fileRealm = Mockito.mock(FileRealm.class);
        Mockito.when(fileRealm.type()).thenReturn("file");
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            @SuppressWarnings("unchecked")
            ActionListener<User> listener = (ActionListener<User>) args[1];
            listener.onResponse(fileRealmUser);
            return null;
        }).when(fileRealm).lookupUser(Mockito.any(String.class), anyActionListener());

        return fileRealm;
    }

    private Realm mockPkiRealm(User foo) {
        PkiRealm ldapRealm = Mockito.mock(PkiRealm.class);
        Mockito.when(ldapRealm.type()).thenReturn(LdapRealmSettings.LDAP_TYPE);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            @SuppressWarnings("unchecked")
            ActionListener<User> listener = (ActionListener<User>) args[1];
            listener.onResponse(foo);
            return null;
        }).when(ldapRealm).lookupUser(Mockito.any(String.class), anyActionListener());

        return ldapRealm;
    }

    private static Realms mockRealms() {
        return mockRealms(mockFileRealm(null));
    }

    private static Realms mockRealms(Realm... realm) {
        Realms realms = mock(Realms.class);
        Mockito.when(realms.stream()).thenReturn(realm == null ? Stream.of() : Stream.of(realm));
        Mockito.when(realms.getActiveRealms()).thenReturn(realm == null ? List.of() : List.of(realm));
        return realms;
    }
}
