/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.authc.InternalAuthenticationService.AuditableRequest;
import org.elasticsearch.shield.SecurityLicenseState.EnabledRealmType;
import org.elasticsearch.shield.user.AnonymousUser;
import org.elasticsearch.shield.user.SystemUser;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.authc.esnative.ReservedRealm;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.crypto.CryptoService;
import org.elasticsearch.shield.SecurityLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;

import static org.elasticsearch.shield.support.Exceptions.authenticationError;
import static org.elasticsearch.test.ShieldTestsUtils.assertAuthenticationException;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;


/**
 *
 */
public class InternalAuthenticationServiceTests extends ESTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    InternalAuthenticationService service;
    TransportMessage message;
    RestRequest restRequest;
    Realms realms;
    Realm firstRealm;
    Realm secondRealm;
    AuditTrail auditTrail;
    AuthenticationToken token;
    CryptoService cryptoService;
    ThreadPool threadPool;
    ThreadContext threadContext;
    RestController controller;

    @Before
    public void init() throws Exception {
        token = mock(AuthenticationToken.class);
        message = new InternalMessage();
        restRequest = new FakeRestRequest();
        firstRealm = mock(Realm.class);
        when(firstRealm.name()).thenReturn("file");
        secondRealm = mock(Realm.class);
        when(secondRealm.name()).thenReturn("second");
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        SecurityLicenseState shieldLicenseState = mock(SecurityLicenseState.class);
        when(shieldLicenseState.enabledRealmType()).thenReturn(EnabledRealmType.ALL);
        realms = new Realms(Settings.EMPTY, new Environment(settings), Collections.<String, Realm.Factory>emptyMap(), shieldLicenseState,
                mock(ReservedRealm.class)) {

            @Override
            protected void doStart() {
                this.realms = Arrays.asList(firstRealm, secondRealm);
                this.internalRealmsOnly = Collections.singletonList(firstRealm);
            }

        };
        realms.start();
        cryptoService = mock(CryptoService.class);

        auditTrail = mock(AuditTrail.class);
        threadPool = mock(ThreadPool.class);
        threadContext = new ThreadContext(Settings.EMPTY);
        controller = mock(RestController.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        service = new InternalAuthenticationService(Settings.EMPTY, realms, auditTrail, cryptoService,
                new DefaultAuthenticationFailureHandler(), threadPool, controller);
    }

    @After
    public void resetAnonymous() {
        AnonymousUser.initialize(Settings.EMPTY);
    }

    @SuppressWarnings("unchecked")
    public void testTokenFirstMissingSecondFound() throws Exception {
        when(firstRealm.token(threadContext)).thenReturn(null);
        when(secondRealm.token(threadContext)).thenReturn(token);

        AuthenticationToken result = service.token(service.newRequest("_action", message));
        assertThat(result, notNullValue());
        assertThat(result, is(token));
        verifyZeroInteractions(auditTrail);
    }

    public void testTokenMissing() throws Exception {
        AuthenticationToken token = service.token(service.newRequest("_action", message));
        assertThat(token, nullValue());
        verifyNoMoreInteractions(auditTrail);
        assertThat(threadContext.getTransient(InternalAuthenticationService.TOKEN_KEY), nullValue());
    }

    @SuppressWarnings("unchecked")
    public void testAuthenticateBothSupportSecondSucceeds() throws Exception {
        User user = new User("_username", "r1");
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.authenticate(token)).thenReturn(null); // first fails
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(user);

        service = spy(service);
        doReturn(token).when(service).token(any(AuditableRequest.class));

        when(cryptoService.sign(InternalAuthenticationService.encodeUser(user, null))).thenReturn("_encoded_user");

        User result = service.authenticate("_action", message, null);
        assertThat(result, notNullValue());
        assertThat(result, is(user));
        verify(auditTrail).authenticationFailed("file", token, "_action", message);
        User user1 = threadContext.getTransient(InternalAuthenticationService.USER_KEY);
        assertThat(user1, notNullValue());
        assertThat(user1, sameInstance(user));
        assertThat(threadContext.getHeader(InternalAuthenticationService.USER_KEY), equalTo((Object) "_encoded_user"));
    }

    public void testAuthenticateFirstNotSupportingSecondSucceeds() throws Exception {
        User user = new User("_username", "r1");
        when(firstRealm.supports(token)).thenReturn(false);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(user);

        service = spy(service);
        doReturn(token).when(service).token(any(AuditableRequest.class));

        when(cryptoService.sign(InternalAuthenticationService.encodeUser(user, null))).thenReturn("_encoded_user");

        User result = service.authenticate("_action", message, null);
        assertThat(result, notNullValue());
        assertThat(result, is(user));
        verifyZeroInteractions(auditTrail);
        verify(firstRealm, never()).authenticate(token);
        User user1 = threadContext.getTransient(InternalAuthenticationService.USER_KEY);
        assertThat(user1, notNullValue());
        assertThat(user1, is((Object) user));
        assertThat(threadContext.getHeader(InternalAuthenticationService.USER_KEY), equalTo((Object) "_encoded_user"));
    }

    public void testAuthenticateCached() throws Exception {
        User user = new User("_username", "r1");
        threadContext.putTransient(InternalAuthenticationService.USER_KEY, user);
        User result = service.authenticate("_action", message, null);
        assertThat(result, notNullValue());
        assertThat(result, is(user));
        verifyZeroInteractions(auditTrail);
        verifyZeroInteractions(firstRealm);
        verifyZeroInteractions(secondRealm);
        verifyZeroInteractions(cryptoService);
        User user1 = threadContext.getTransient(InternalAuthenticationService.USER_KEY);
        assertThat(user1, notNullValue());
        assertThat(user1, is(user));
    }

    public void testAuthenticateNonExistentRestRequestUserThrowsAuthenticationException() throws Exception {
        when(firstRealm.token(threadContext)).thenReturn(new UsernamePasswordToken("idonotexist",
                new SecuredString("passwd".toCharArray())));
        try {
            service.authenticate(restRequest);
            fail("Authentication was successful but should not");
        } catch (ElasticsearchSecurityException e) {
            assertAuthenticationException(e, containsString("unable to authenticate user [idonotexist] for REST request [/]"));
        }
    }

    public void testTokenRestMissing() throws Exception {
        when(firstRealm.token(threadContext)).thenReturn(null);
        when(secondRealm.token(threadContext)).thenReturn(null);
        AuthenticationToken token = service.token(service.newRequest(restRequest));
        assertThat(token, nullValue());
    }

    public void testEncodeDecodeUser() throws Exception {
        User user = new User("username", "r1", "r2", "r3");
        String text = InternalAuthenticationService.encodeUser(user, null);
        User user2 = InternalAuthenticationService.decodeUser(text);
        assertThat(user, equalTo(user2));

        text = InternalAuthenticationService.encodeUser(SystemUser.INSTANCE, null);
        user2 = InternalAuthenticationService.decodeUser(text);
        assertThat(SystemUser.INSTANCE, sameInstance(user2));
    }

    public void testUserHeader() throws Exception {
        User user = new User("_username", "r1");
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.authenticate(token)).thenReturn(user);
        when(cryptoService.sign(InternalAuthenticationService.encodeUser(user, null))).thenReturn("_signed_user");
        service = spy(service);
        AuditableRequest request = service.newRequest("_action", message);
        doReturn(token).when(service).token(request);
        User result = service.authenticate("_action", message, null);
        assertThat(result, notNullValue());
        assertThat(result, is(user));
        String userStr = threadContext.getHeader(InternalAuthenticationService.USER_KEY);
        assertThat(userStr, notNullValue());
        assertThat(userStr, equalTo("_signed_user"));
    }

    public void testAuthenticateTransportAnonymous() throws Exception {
        when(firstRealm.token(threadContext)).thenReturn(null);
        when(secondRealm.token(threadContext)).thenReturn(null);
        try {
            service.authenticate("_action", message, null);
            fail("expected an authentication exception when trying to authenticate an anonymous message");
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertAuthenticationException(e);
        }
        verify(auditTrail).anonymousAccessDenied("_action", message);
    }

    public void testAuthenticateRestAnonymous()  throws Exception {
        when(firstRealm.token(threadContext)).thenReturn(null);
        when(secondRealm.token(threadContext)).thenReturn(null);
        try {
            service.authenticate(restRequest);
            fail("expected an authentication exception when trying to authenticate an anonymous message");
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertAuthenticationException(e);
        }
        verify(auditTrail).anonymousAccessDenied(restRequest);
    }

    public void testAuthenticateTransportFallback() throws Exception {
        when(firstRealm.token(threadContext)).thenReturn(null);
        when(secondRealm.token(threadContext)).thenReturn(null);
        User user1 = new User("username", "r1", "r2");
        when(cryptoService.sign(InternalAuthenticationService.encodeUser(user1, null))).thenReturn("_signed_user");
        User user2 = service.authenticate("_action", message, user1);
        assertThat(user1, sameInstance(user2));
        User user3 = threadContext.getTransient(InternalAuthenticationService.USER_KEY);
        assertThat(user3, sameInstance(user2));
        assertThat(threadContext.getHeader(InternalAuthenticationService.USER_KEY), equalTo((Object) "_signed_user"));
    }

    public void testAuthenticateTransportSuccessNoFallback() throws Exception {
        User user1 = new User("username", "r1", "r2");
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.authenticate(token)).thenReturn(user1);
        when(cryptoService.sign(InternalAuthenticationService.encodeUser(user1, null))).thenReturn("_signed_user");
        User user2 = service.authenticate("_action", message, null);
        assertThat(user1, sameInstance(user2));
        User user3 = threadContext.getTransient(InternalAuthenticationService.USER_KEY);
        assertThat(user3, sameInstance(user2));
        assertThat(threadContext.getHeader(InternalAuthenticationService.USER_KEY), equalTo("_signed_user"));
    }

    public void testAuthenticateTransportSuccessWithFallback() throws Exception {
        User user1 = new User("username", "r1", "r2");
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.authenticate(token)).thenReturn(user1);
        when(cryptoService.sign(InternalAuthenticationService.encodeUser(user1, null))).thenReturn("_signed_user");
        User user2 = service.authenticate("_action", message, SystemUser.INSTANCE);
        assertThat(user1, sameInstance(user2));
        User user3 = threadContext.getTransient(InternalAuthenticationService.USER_KEY);
        assertThat(user3, sameInstance((Object) user2));
        assertThat(threadContext.getHeader(InternalAuthenticationService.USER_KEY), equalTo((Object) "_signed_user"));
    }

    public void testAuthenticateRestSuccess() throws Exception {
        User user1 = new User("username", "r1", "r2");
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.authenticate(token)).thenReturn(user1);
        User user2 = service.authenticate(restRequest);
        assertThat(user1, sameInstance(user2));
        User user3 = threadContext.getTransient(InternalAuthenticationService.USER_KEY);
        assertThat(user3, sameInstance(user2));
    }

    public void testAutheticateTransportContextAndHeader() throws Exception {
        User user1 = new User("username", "r1", "r2");
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.authenticate(token)).thenReturn(user1);
        when(cryptoService.sign(InternalAuthenticationService.encodeUser(user1, null))).thenReturn("_signed_user");
        User user2 = service.authenticate("_action", message, SystemUser.INSTANCE);
        assertThat(user1, sameInstance(user2));
        User user3 = threadContext.getTransient(InternalAuthenticationService.USER_KEY);
        assertThat(user3, sameInstance(user2));
        assertThat(threadContext.getHeader(InternalAuthenticationService.USER_KEY), equalTo((Object) "_signed_user"));
        reset(firstRealm);

        // checking authentication from the context
        InternalMessage message1 = new InternalMessage();
        ThreadContext threadContext1 = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext1);
        service = new InternalAuthenticationService(Settings.EMPTY, realms, auditTrail, cryptoService,
                new DefaultAuthenticationFailureHandler(), threadPool, controller);

        threadContext1.putTransient(InternalAuthenticationService.USER_KEY,
                threadContext.getTransient(InternalAuthenticationService.USER_KEY));
        User user = service.authenticate("_action", message1, SystemUser.INSTANCE);
        assertThat(user, sameInstance(user1));
        verifyZeroInteractions(firstRealm);
        reset(firstRealm);


        // checking authentication from the user header
        threadContext1 = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext1);
        service = new InternalAuthenticationService(Settings.EMPTY, realms, auditTrail, cryptoService,
                new DefaultAuthenticationFailureHandler(), threadPool, controller);
        threadContext1.putHeader(InternalAuthenticationService.USER_KEY, threadContext.getHeader(InternalAuthenticationService.USER_KEY));
        when(cryptoService.unsignAndVerify("_signed_user")).thenReturn(InternalAuthenticationService.encodeUser(user1, null));

        BytesStreamOutput output = new BytesStreamOutput();
        threadContext1.writeTo(output);
        StreamInput input = StreamInput.wrap(output.bytes());
        threadContext1 = new ThreadContext(Settings.EMPTY);
        threadContext1.readHeaders(input);

        when(threadPool.getThreadContext()).thenReturn(threadContext1);
        service = new InternalAuthenticationService(Settings.EMPTY, realms, auditTrail, cryptoService,
                new DefaultAuthenticationFailureHandler(), threadPool, controller);
        user = service.authenticate("_action", new InternalMessage(), SystemUser.INSTANCE);
        assertThat(user, equalTo(user1));
        verifyZeroInteractions(firstRealm);
    }

    public void testAutheticateTransportContextAndHeaderNoSigning() throws Exception {
        Settings settings = Settings.builder().put(InternalAuthenticationService.SIGN_USER_HEADER.getKey(), false).build();
        service = new InternalAuthenticationService(settings, realms, auditTrail, cryptoService,
                new DefaultAuthenticationFailureHandler(), threadPool, controller);

        User user1 = new User("username", "r1", "r2");
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.authenticate(token)).thenReturn(user1);
        User user2 = service.authenticate("_action", message, SystemUser.INSTANCE);
        assertThat(user1, sameInstance(user2));
        User user3 = threadContext.getTransient(InternalAuthenticationService.USER_KEY);
        assertThat(user3, sameInstance(user2));
        assertThat(threadContext.getHeader(InternalAuthenticationService.USER_KEY),
                equalTo((Object) InternalAuthenticationService.encodeUser(user1, null)));
        reset(firstRealm);

        // checking authentication from the context
        InternalMessage message1 = new InternalMessage();
        ThreadContext threadContext1 = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext1);
        service = new InternalAuthenticationService(Settings.EMPTY, realms, auditTrail, cryptoService,
                new DefaultAuthenticationFailureHandler(), threadPool, controller);
        threadContext1.putTransient(InternalAuthenticationService.USER_KEY,
                threadContext.getTransient(InternalAuthenticationService.USER_KEY));
        User user = service.authenticate("_action", message1, SystemUser.INSTANCE);
        assertThat(user, sameInstance(user1));
        verifyZeroInteractions(firstRealm);
        reset(firstRealm);


        // checking authentication from the user header
        threadContext1 = new ThreadContext(Settings.EMPTY);
        threadContext1.putHeader(InternalAuthenticationService.USER_KEY, threadContext.getHeader(InternalAuthenticationService.USER_KEY));

        BytesStreamOutput output = new BytesStreamOutput();
        threadContext1.writeTo(output);
        StreamInput input = StreamInput.wrap(output.bytes());
        threadContext1 = new ThreadContext(Settings.EMPTY);
        threadContext1.readHeaders(input);

        when(threadPool.getThreadContext()).thenReturn(threadContext1);
        service = new InternalAuthenticationService(settings, realms, auditTrail, cryptoService,
                new DefaultAuthenticationFailureHandler(), threadPool, controller);
        user = service.authenticate("_action", new InternalMessage(), SystemUser.INSTANCE);
        assertThat(user, equalTo(user1));
        verifyZeroInteractions(firstRealm);

        verifyZeroInteractions(cryptoService);
    }

    public void testAuthenticateTamperedUser() throws Exception {
        InternalMessage message = new InternalMessage();
        threadContext.putHeader(InternalAuthenticationService.USER_KEY, "_signed_user");
        when(cryptoService.unsignAndVerify("_signed_user")).thenThrow(
                randomFrom(new RuntimeException(), new IllegalArgumentException(), new IllegalStateException()));

        try {
            service.authenticate("_action", message, randomBoolean() ? SystemUser.INSTANCE : null);
        } catch (Exception e) {
            //expected
            verify(auditTrail).tamperedRequest("_action", message);
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testAttachIfMissing() throws Exception {
        User user;
        if (randomBoolean()) {
            user = SystemUser.INSTANCE;
        } else {
            user = new User("username", "r1", "r2");
        }
        assertThat(threadContext.getTransient(InternalAuthenticationService.USER_KEY), nullValue());
        assertThat(threadContext.getHeader(InternalAuthenticationService.USER_KEY), nullValue());
        when(cryptoService.sign(InternalAuthenticationService.encodeUser(user, null))).thenReturn("_signed_user");
        service.attachUserHeaderIfMissing(user);
        User user1 = threadContext.getTransient(InternalAuthenticationService.USER_KEY);
        assertThat(user1, sameInstance((Object) user));
        assertThat(threadContext.getHeader(InternalAuthenticationService.USER_KEY), equalTo((Object) "_signed_user"));
    }

    public void testAttachIfMissingExists() throws Exception {
        User user = new User("username", "r1", "r2");
        threadContext.putTransient(InternalAuthenticationService.USER_KEY, user);
        threadContext.putHeader(InternalAuthenticationService.USER_KEY, "_signed_user");
        service.attachUserHeaderIfMissing(new User("username2", "r3", "r4"));
        User user1 = threadContext.getTransient(InternalAuthenticationService.USER_KEY);
        assertThat(user1, sameInstance(user));
        assertThat(threadContext.getHeader(InternalAuthenticationService.USER_KEY), equalTo("_signed_user"));
    }

    public void testAnonymousUserRest() throws Exception {
        String username = randomBoolean() ? AnonymousUser.DEFAULT_ANONYMOUS_USERNAME : "user1";
        Settings.Builder builder = Settings.builder()
                .putArray(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3");
        if (username.equals(AnonymousUser.DEFAULT_ANONYMOUS_USERNAME) == false) {
            builder.put(AnonymousUser.USERNAME_SETTING.getKey(), username);
        }
        Settings settings = builder.build();
        AnonymousUser.initialize(settings);
        service = new InternalAuthenticationService(settings, realms, auditTrail, cryptoService, new DefaultAuthenticationFailureHandler(),
                threadPool, controller);

        RestRequest request = new FakeRestRequest();

        User user = service.authenticate(request);
        User user1 = threadContext.getTransient(InternalAuthenticationService.USER_KEY);
        assertThat(user1, notNullValue());
        assertThat(user1, sameInstance((Object) user));
        assertThat(user, notNullValue());
        assertThat(user.principal(), equalTo(username));
        assertThat(user.roles(), arrayContainingInAnyOrder("r1", "r2", "r3"));
    }

    public void testAnonymousUserTransportNoDefaultUser() throws Exception {
        Settings settings = Settings.builder()
                .putArray(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3")
                .build();
        AnonymousUser.initialize(settings);
        service = new InternalAuthenticationService(settings, realms, auditTrail, cryptoService,
                new DefaultAuthenticationFailureHandler(), threadPool, controller);

        InternalMessage message = new InternalMessage();

        User user = service.authenticate("_action", message, null);
        assertThat(user, notNullValue());
        assertThat(user.principal(), equalTo(AnonymousUser.DEFAULT_ANONYMOUS_USERNAME));
        assertThat(user.roles(), arrayContainingInAnyOrder("r1", "r2", "r3"));
    }

    public void testAnonymousUserTransportWithDefaultUser() throws Exception {
        Settings settings = Settings.builder()
                .putArray(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3")
                .build();
        AnonymousUser.initialize(settings);
        service = new InternalAuthenticationService(settings, realms, auditTrail, cryptoService,
                new DefaultAuthenticationFailureHandler(), threadPool, controller);

        InternalMessage message = new InternalMessage();

        User user = service.authenticate("_action", message, SystemUser.INSTANCE);
        assertThat(user, notNullValue());
        assertThat(user, sameInstance(SystemUser.INSTANCE));
    }

    public void testRealmTokenThrowingException() throws Exception {
        when(firstRealm.token(threadContext)).thenThrow(authenticationError("realm doesn't like tokens"));
        try {
            service.authenticate("_action", message, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like tokens"));
            verify(auditTrail).authenticationFailed("_action", message);
        }
    }

    public void testRealmTokenThrowingExceptionRest() throws Exception {
        when(firstRealm.token(threadContext)).thenThrow(authenticationError("realm doesn't like tokens"));
        try {
            service.authenticate(restRequest);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like tokens"));
            verify(auditTrail).authenticationFailed(restRequest);
        }
    }

    public void testRealmSupportsMethodThrowingException() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenThrow(authenticationError("realm doesn't like supports"));
        try {
            service.authenticate("_action", message, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like supports"));
            verify(auditTrail).authenticationFailed(token, "_action", message);
        }
    }

    public void testRealmSupportsMethodThrowingExceptionRest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenThrow(authenticationError("realm doesn't like supports"));
        try {
            service.authenticate(restRequest);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like supports"));
            verify(auditTrail).authenticationFailed(token, restRequest);
        }
    }

    public void testRealmAuthenticateThrowingException() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenThrow(authenticationError("realm doesn't like authenticate"));
        try {
            service.authenticate("_action", message, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like authenticate"));
            verify(auditTrail).authenticationFailed(token, "_action", message);
        }
    }

    public void testRealmAuthenticateThrowingExceptionRest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenThrow(authenticationError("realm doesn't like authenticate"));
        try {
            service.authenticate(restRequest);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like authenticate"));
            verify(auditTrail).authenticationFailed(token, restRequest);
        }
    }

    public void testRealmLookupThrowingException() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        threadContext.putHeader(InternalAuthenticationService.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(new User("lookup user", new String[]{"user"}));
        when(secondRealm.lookupUser("run_as")).thenThrow(authenticationError("realm doesn't want to lookup"));
        when(secondRealm.userLookupSupported()).thenReturn(true);

        try {
            service.authenticate("_action", message, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't want to lookup"));
            verify(auditTrail).authenticationFailed(token, "_action", message);
        }
    }

    public void testRealmLookupThrowingExceptionRest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        threadContext.putHeader(InternalAuthenticationService.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(new User("lookup user", new String[]{"user"}));
        when(secondRealm.lookupUser("run_as")).thenThrow(authenticationError("realm doesn't want to lookup"));
        when(secondRealm.userLookupSupported()).thenReturn(true);

        try {
            service.authenticate(restRequest);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't want to lookup"));
            verify(auditTrail).authenticationFailed(token, restRequest);
        }
    }

    public void testRunAsLookupSameRealm() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        threadContext.putHeader(InternalAuthenticationService.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(new User("lookup user", new String[]{"user"}));
        when(secondRealm.lookupUser("run_as")).thenReturn(new User("looked up user", new String[]{"some role"}));
        when(secondRealm.userLookupSupported()).thenReturn(true);

        User authenticated = service.authenticate("_action", message, null);

        assertThat(SystemUser.is(authenticated), is(false));
        assertThat(authenticated.runAs(), is(notNullValue()));
        assertThat(authenticated.principal(), is("lookup user"));
        assertThat(authenticated.roles(), arrayContaining("user"));
        assertThat(authenticated.runAs().principal(), is("looked up user"));
        assertThat(authenticated.runAs().roles(), arrayContaining("some role"));
        User user1 = threadContext.getTransient(InternalAuthenticationService.USER_KEY);
        assertThat(user1, sameInstance(authenticated));
    }

    public void testRunAsLookupSameRealmRest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        threadContext.putHeader(InternalAuthenticationService.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(new User("lookup user", new String[]{"user"}));
        when(secondRealm.lookupUser("run_as")).thenReturn(new User("looked up user", new String[]{"some role"}));
        when(secondRealm.userLookupSupported()).thenReturn(true);

        User authenticated = service.authenticate(restRequest);

        assertThat(SystemUser.is(authenticated), is(false));
        assertThat(authenticated.runAs(), is(notNullValue()));
        assertThat(authenticated.principal(), is("lookup user"));
        assertThat(authenticated.roles(), arrayContaining("user"));
        assertThat(authenticated.runAs().principal(), is("looked up user"));
        assertThat(authenticated.runAs().roles(), arrayContaining("some role"));
        User user1 = threadContext.getTransient(InternalAuthenticationService.USER_KEY);
        assertThat(user1, sameInstance(authenticated));
    }

    public void testRunAsLookupDifferentRealm() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        threadContext.putHeader(InternalAuthenticationService.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(new User("lookup user", new String[]{"user"}));
        when(firstRealm.userLookupSupported()).thenReturn(true);
        when(firstRealm.lookupUser("run_as")).thenReturn(new User("looked up user", new String[]{"some role"}));
        when(firstRealm.userLookupSupported()).thenReturn(true);

        User authenticated = service.authenticate("_action", message, null);

        assertThat(SystemUser.is(authenticated), is(false));
        assertThat(authenticated.runAs(), is(notNullValue()));
        assertThat(authenticated.principal(), is("lookup user"));
        assertThat(authenticated.roles(), arrayContaining("user"));
        assertThat(authenticated.runAs().principal(), is("looked up user"));
        assertThat(authenticated.runAs().roles(), arrayContaining("some role"));
        User user1 = threadContext.getTransient(InternalAuthenticationService.USER_KEY);
        assertThat(user1, sameInstance(authenticated));
    }

    public void testRunAsLookupDifferentRealmRest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        threadContext.putHeader(InternalAuthenticationService.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(new User("lookup user", new String[]{"user"}));
        when(firstRealm.lookupUser("run_as")).thenReturn(new User("looked up user", new String[]{"some role"}));
        when(firstRealm.userLookupSupported()).thenReturn(true);

        User authenticated = service.authenticate(restRequest);

        assertThat(SystemUser.is(authenticated), is(false));
        assertThat(authenticated.runAs(), is(notNullValue()));
        assertThat(authenticated.principal(), is("lookup user"));
        assertThat(authenticated.roles(), arrayContaining("user"));
        assertThat(authenticated.runAs().principal(), is("looked up user"));
        assertThat(authenticated.runAs().roles(), arrayContaining("some role"));
        User user1 = threadContext.getTransient(InternalAuthenticationService.USER_KEY);
        assertThat(user1, sameInstance(authenticated));
    }

    public void testRunAsWithEmptyRunAsUsernameRest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        threadContext.putHeader(InternalAuthenticationService.RUN_AS_USER_HEADER, "");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(new User("lookup user", new String[]{"user"}));
        when(secondRealm.userLookupSupported()).thenReturn(true);

        try {
            service.authenticate(restRequest);
            fail("exception should be thrown");
        } catch (ElasticsearchException e) {
            verify(auditTrail).authenticationFailed(token, restRequest);
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testRunAsWithEmptyRunAsUsername() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        threadContext.putHeader(InternalAuthenticationService.RUN_AS_USER_HEADER, "");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(new User("lookup user", new String[]{"user"}));
        when(secondRealm.userLookupSupported()).thenReturn(true);

        try {
            service.authenticate("_action", message, null);
            fail("exception should be thrown");
        } catch (ElasticsearchException e) {
            verify(auditTrail).authenticationFailed(token, "_action", message);
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testVersionWrittenWithUser() throws Exception {
        User user = new User("username", "r1", "r2", "r3");
        String text = InternalAuthenticationService.encodeUser(user, null);

        StreamInput input = StreamInput.wrap(Base64.decode(text));
        Version version = Version.readVersion(input);
        assertThat(version, is(Version.CURRENT));
    }

    private static class InternalMessage extends TransportMessage {
    }
}
