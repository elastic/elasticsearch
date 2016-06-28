/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.security.authc.InternalAuthenticationService.Authenticator;
import org.elasticsearch.xpack.security.SecurityLicenseState.EnabledRealmType;
import org.elasticsearch.xpack.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.crypto.CryptoService;
import org.elasticsearch.xpack.security.SecurityLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportMessage;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.elasticsearch.xpack.security.support.Exceptions.authenticationError;
import static org.elasticsearch.test.SecurityTestsUtils.assertAuthenticationException;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;


/**
 *
 */
public class InternalAuthenticationServiceTests extends ESTestCase {

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
        when(firstRealm.type()).thenReturn("file");
        when(firstRealm.name()).thenReturn("file_realm");
        secondRealm = mock(Realm.class);
        when(secondRealm.type()).thenReturn("second");
        when(secondRealm.name()).thenReturn("second_realm");
        Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put("node.name", "authc_test")
                .build();
        SecurityLicenseState securityLicenseState = mock(SecurityLicenseState.class);
        when(securityLicenseState.enabledRealmType()).thenReturn(EnabledRealmType.ALL);
        when(securityLicenseState.authenticationAndAuthorizationEnabled()).thenReturn(true);
        realms = new Realms(Settings.EMPTY, new Environment(settings), Collections.<String, Realm.Factory>emptyMap(), securityLicenseState,
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
        when(cryptoService.sign(any(String.class))).thenReturn("_signed_auth");
        service = new InternalAuthenticationService(settings, realms, auditTrail, cryptoService,
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

        Authenticator authenticator = service.createAuthenticator("_action", message, null);
        AuthenticationToken result = authenticator.extractToken();
        assertThat(result, notNullValue());
        assertThat(result, is(token));
        verifyZeroInteractions(auditTrail);
    }

    public void testTokenMissing() throws Exception {
        Authenticator authenticator = service.createAuthenticator("_action", message, null);
        AuthenticationToken token = authenticator.extractToken();
        assertThat(token, nullValue());
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, authenticator::handleNullToken);
        assertThat(e.getMessage(), containsString("missing authentication token"));
        verify(auditTrail).anonymousAccessDenied("_action", message);
        verifyNoMoreInteractions(auditTrail);
    }

    @SuppressWarnings("unchecked")
    public void testAuthenticateBothSupportSecondSucceeds() throws Exception {
        User user = new User("_username", "r1");
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.authenticate(token)).thenReturn(null); // first fails
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(user);
        if (randomBoolean()) {
            when(firstRealm.token(threadContext)).thenReturn(token);
        } else {
            when(secondRealm.token(threadContext)).thenReturn(token);
        }

        Authentication result = service.authenticate("_action", message, null);
        assertThat(result, notNullValue());
        assertThat(result.getUser(), is(user));
        assertThat(result.getLookedUpBy(), is(nullValue()));
        assertThat(result.getAuthenticatedBy(), is(notNullValue())); // TODO implement equals
        verify(auditTrail).authenticationFailed(firstRealm.name(), token, "_action", message);
        verify(cryptoService).sign(any(String.class));
        assertThreadContextContainsAuthentication(result);
    }

    public void testAuthenticateFirstNotSupportingSecondSucceeds() throws Exception {
        User user = new User("_username", "r1");
        when(firstRealm.supports(token)).thenReturn(false);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(user);
        when(secondRealm.token(threadContext)).thenReturn(token);

        Authentication result = service.authenticate("_action", message, null);
        assertThat(result, notNullValue());
        assertThat(result.getUser(), is(user));
        verifyZeroInteractions(auditTrail);
        verify(firstRealm, never()).authenticate(token);
        assertThreadContextContainsAuthentication(result);
    }

    public void testAuthenticateCached() throws Exception {
        final Authentication authentication = new Authentication(new User("_username", "r1"), new RealmRef("test", "cached", "foo"), null);
        authentication.writeToContext(threadContext, cryptoService, true);

        Authentication result = service.authenticate("_action", message, null);

        assertThat(result, notNullValue());
        assertThat(result, is(authentication));
        verifyZeroInteractions(auditTrail);
        verifyZeroInteractions(firstRealm);
        verifyZeroInteractions(secondRealm);
        verify(cryptoService).sign(any(String.class));
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

        Authenticator authenticator = service.createAuthenticator(restRequest);
        AuthenticationToken token = authenticator.extractToken();

        assertThat(token, nullValue());
    }

    public void authenticationInContextAndHeader() throws Exception {
        User user = new User("_username", "r1");
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.authenticate(token)).thenReturn(user);

        Authentication result = service.authenticate("_action", message, null);

        assertThat(result, notNullValue());
        assertThat(result.getUser(), is(user));

        String userStr = threadContext.getHeader(Authentication.AUTHENTICATION_KEY);
        assertThat(userStr, notNullValue());
        assertThat(userStr, equalTo("_signed_auth"));

        Authentication ctxAuth = threadContext.getTransient(Authentication.AUTHENTICATION_KEY);
        assertThat(ctxAuth, is(result));
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

        Authentication result = service.authenticate("_action", message, user1);
        assertThat(result, notNullValue());
        assertThat(result.getUser(), sameInstance(user1));
        assertThreadContextContainsAuthentication(result);
    }

    public void testAuthenticateTransportSuccess() throws Exception {
        User user = new User("username", "r1", "r2");
        User fallback = randomBoolean() ? SystemUser.INSTANCE : null;
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.authenticate(token)).thenReturn(user);

        Authentication result = service.authenticate("_action", message, fallback);
        assertThat(result, notNullValue());
        assertThat(result.getUser(), sameInstance(user));
        assertThreadContextContainsAuthentication(result);
    }

    public void testAuthenticateRestSuccess() throws Exception {
        User user1 = new User("username", "r1", "r2");
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.authenticate(token)).thenReturn(user1);
        Authentication result = service.authenticate(restRequest);
        assertThat(result, notNullValue());
        assertThat(result.getUser(), sameInstance(user1));
        assertThreadContextContainsAuthentication(result);
    }

    public void testAutheticateTransportContextAndHeader() throws Exception {
        User user1 = new User("username", "r1", "r2");
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.authenticate(token)).thenReturn(user1);
        Authentication authentication = service.authenticate("_action", message, SystemUser.INSTANCE);
        assertThat(authentication, notNullValue());
        assertThat(authentication.getUser(), sameInstance(user1));
        assertThreadContextContainsAuthentication(authentication);
        reset(firstRealm);

        // checking authentication from the context
        InternalMessage message1 = new InternalMessage();
        ThreadContext threadContext1 = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext1);
        service = new InternalAuthenticationService(Settings.EMPTY, realms, auditTrail, cryptoService,
                new DefaultAuthenticationFailureHandler(), threadPool, controller);

        threadContext1.putTransient(Authentication.AUTHENTICATION_KEY, threadContext.getTransient(Authentication.AUTHENTICATION_KEY));
        threadContext1.putHeader(Authentication.AUTHENTICATION_KEY, threadContext.getHeader(Authentication.AUTHENTICATION_KEY));
        Authentication ctxAuth = service.authenticate("_action", message1, SystemUser.INSTANCE);
        assertThat(ctxAuth, sameInstance(authentication));
        verifyZeroInteractions(firstRealm);
        reset(firstRealm);


        // checking authentication from the user header
        threadContext1 = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext1);
        service = new InternalAuthenticationService(Settings.EMPTY, realms, auditTrail, cryptoService,
                new DefaultAuthenticationFailureHandler(), threadPool, controller);
        threadContext1.putHeader(Authentication.AUTHENTICATION_KEY, threadContext.getHeader(Authentication.AUTHENTICATION_KEY));
        when(cryptoService.unsignAndVerify("_signed_auth")).thenReturn(authentication.encode());

        BytesStreamOutput output = new BytesStreamOutput();
        threadContext1.writeTo(output);
        StreamInput input = StreamInput.wrap(output.bytes());
        threadContext1 = new ThreadContext(Settings.EMPTY);
        threadContext1.readHeaders(input);

        when(threadPool.getThreadContext()).thenReturn(threadContext1);
        service = new InternalAuthenticationService(Settings.EMPTY, realms, auditTrail, cryptoService,
                new DefaultAuthenticationFailureHandler(), threadPool, controller);
        Authentication result = service.authenticate("_action", new InternalMessage(), SystemUser.INSTANCE);
        assertThat(result, notNullValue());
        assertThat(result.getUser(), equalTo(user1));
        verifyZeroInteractions(firstRealm);
    }

    public void testAuthenticateTransportContextAndHeaderNoSigning() throws Exception {
        Settings settings = Settings.builder().put(InternalAuthenticationService.SIGN_USER_HEADER.getKey(), false).build();
        service = new InternalAuthenticationService(settings, realms, auditTrail, cryptoService,
                new DefaultAuthenticationFailureHandler(), threadPool, controller);

        User user1 = new User("username", "r1", "r2");
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.authenticate(token)).thenReturn(user1);
        Authentication authentication = service.authenticate("_action", message, SystemUser.INSTANCE);
        assertThat(authentication, notNullValue());
        assertThat(authentication.getUser(), sameInstance(user1));
        assertThreadContextContainsAuthentication(authentication, false);
        reset(firstRealm);

        // checking authentication from the context
        InternalMessage message1 = new InternalMessage();
        ThreadContext threadContext1 = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext1);
        service = new InternalAuthenticationService(Settings.EMPTY, realms, auditTrail, cryptoService,
                new DefaultAuthenticationFailureHandler(), threadPool, controller);
        threadContext1.putTransient(Authentication.AUTHENTICATION_KEY, threadContext.getTransient(Authentication.AUTHENTICATION_KEY));
        threadContext1.putHeader(Authentication.AUTHENTICATION_KEY, threadContext.getHeader(Authentication.AUTHENTICATION_KEY));
        Authentication ctxAuth = service.authenticate("_action", message1, SystemUser.INSTANCE);
        assertThat(ctxAuth, sameInstance(authentication));
        verifyZeroInteractions(firstRealm);
        reset(firstRealm);

        // checking authentication from the user header
        threadContext1 = new ThreadContext(Settings.EMPTY);
        threadContext1.putHeader(Authentication.AUTHENTICATION_KEY, threadContext.getHeader(Authentication.AUTHENTICATION_KEY));

        BytesStreamOutput output = new BytesStreamOutput();
        threadContext1.writeTo(output);
        StreamInput input = StreamInput.wrap(output.bytes());
        threadContext1 = new ThreadContext(Settings.EMPTY);
        threadContext1.readHeaders(input);

        when(threadPool.getThreadContext()).thenReturn(threadContext1);
        service = new InternalAuthenticationService(settings, realms, auditTrail, cryptoService,
                new DefaultAuthenticationFailureHandler(), threadPool, controller);
        Authentication result = service.authenticate("_action", new InternalMessage(), SystemUser.INSTANCE);
        assertThat(result, notNullValue());
        assertThat(result.getUser(), equalTo(user1));
        verifyZeroInteractions(firstRealm);

        verifyZeroInteractions(cryptoService);
    }

    public void testAuthenticateTamperedUser() throws Exception {
        InternalMessage message = new InternalMessage();
        threadContext.putHeader(Authentication.AUTHENTICATION_KEY, "_signed_auth");
        when(cryptoService.unsignAndVerify("_signed_auth")).thenThrow(
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
        assertThat(threadContext.getTransient(Authentication.AUTHENTICATION_KEY), nullValue());
        assertThat(threadContext.getHeader(Authentication.AUTHENTICATION_KEY), nullValue());
        service.attachUserIfMissing(user);

        Authentication authentication = threadContext.getTransient(Authentication.AUTHENTICATION_KEY);
        assertThat(authentication, notNullValue());
        assertThat(authentication.getUser(), sameInstance((Object) user));
        assertThat(authentication.getLookedUpBy(), nullValue());
        assertThat(authentication.getAuthenticatedBy().getName(), is("__attach"));
        assertThat(authentication.getAuthenticatedBy().getType(), is("__attach"));
        assertThat(authentication.getAuthenticatedBy().getNodeName(), is("authc_test"));
        assertThat(threadContext.getHeader(Authentication.AUTHENTICATION_KEY), equalTo((Object) "_signed_auth"));
    }

    public void testAttachIfMissingExists() throws Exception {
        Authentication authentication = new Authentication(new User("username", "r1", "r2"), new RealmRef("test", "test", "foo"), null);
        threadContext.putTransient(Authentication.AUTHENTICATION_KEY, authentication);
        threadContext.putHeader(Authentication.AUTHENTICATION_KEY, "_signed_auth");
        service.attachUserIfMissing(new User("username2", "r3", "r4"));
        assertThreadContextContainsAuthentication(authentication);
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

        Authentication result = service.authenticate(request);

        assertThat(result, notNullValue());
        assertThat(result.getUser(), sameInstance((Object) AnonymousUser.INSTANCE));
        assertThreadContextContainsAuthentication(result);
    }

    public void testAnonymousUserTransportNoDefaultUser() throws Exception {
        Settings settings = Settings.builder()
                .putArray(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3")
                .build();
        AnonymousUser.initialize(settings);
        service = new InternalAuthenticationService(settings, realms, auditTrail, cryptoService,
                new DefaultAuthenticationFailureHandler(), threadPool, controller);
        InternalMessage message = new InternalMessage();

        Authentication result = service.authenticate("_action", message, null);
        assertThat(result, notNullValue());
        assertThat(result.getUser(), sameInstance(AnonymousUser.INSTANCE));
        assertThreadContextContainsAuthentication(result);
    }

    public void testAnonymousUserTransportWithDefaultUser() throws Exception {
        Settings settings = Settings.builder()
                .putArray(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3")
                .build();
        AnonymousUser.initialize(settings);
        service = new InternalAuthenticationService(settings, realms, auditTrail, cryptoService,
                new DefaultAuthenticationFailureHandler(), threadPool, controller);

        InternalMessage message = new InternalMessage();

        Authentication result = service.authenticate("_action", message, SystemUser.INSTANCE);
        assertThat(result, notNullValue());
        assertThat(result.getUser(), sameInstance(SystemUser.INSTANCE));
        assertThreadContextContainsAuthentication(result);
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

        Authentication result;
        if (randomBoolean()) {
            result = service.authenticate("_action", message, null);
        } else {
            result = service.authenticate(restRequest);
        }
        assertThat(result, notNullValue());
        User authenticated = result.getUser();

        assertThat(SystemUser.is(authenticated), is(false));
        assertThat(authenticated.runAs(), is(notNullValue()));
        assertThat(authenticated.principal(), is("lookup user"));
        assertThat(authenticated.roles(), arrayContaining("user"));
        assertThat(authenticated.runAs().principal(), is("looked up user"));
        assertThat(authenticated.runAs().roles(), arrayContaining("some role"));
        assertThreadContextContainsAuthentication(result);
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

        Authentication result;
        if (randomBoolean()) {
            result = service.authenticate("_action", message, null);
        } else {
            result = service.authenticate(restRequest);
        }
        assertThat(result, notNullValue());
        User authenticated = result.getUser();

        assertThat(SystemUser.is(authenticated), is(false));
        assertThat(authenticated.runAs(), is(notNullValue()));
        assertThat(authenticated.principal(), is("lookup user"));
        assertThat(authenticated.roles(), arrayContaining("user"));
        assertThat(authenticated.runAs().principal(), is("looked up user"));
        assertThat(authenticated.runAs().roles(), arrayContaining("some role"));
        assertThreadContextContainsAuthentication(result);
    }

    public void testRunAsWithEmptyRunAsUsernameRest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        User user = new User("lookup user", new String[]{"user"});
        threadContext.putHeader(InternalAuthenticationService.RUN_AS_USER_HEADER, "");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(user);
        when(secondRealm.userLookupSupported()).thenReturn(true);

        try {
            service.authenticate(restRequest);
            fail("exception should be thrown");
        } catch (ElasticsearchException e) {
            verify(auditTrail).runAsDenied(any(User.class), eq(restRequest));
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testRunAsWithEmptyRunAsUsername() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        User user = new User("lookup user", new String[]{"user"});
        threadContext.putHeader(InternalAuthenticationService.RUN_AS_USER_HEADER, "");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(user);
        when(secondRealm.userLookupSupported()).thenReturn(true);

        try {
            service.authenticate("_action", message, null);
            fail("exception should be thrown");
        } catch (ElasticsearchException e) {
            verify(auditTrail).runAsDenied(any(User.class), eq("_action"), eq(message));
            verifyNoMoreInteractions(auditTrail);
        }
    }

    private static class InternalMessage extends TransportMessage {
    }

    void assertThreadContextContainsAuthentication(Authentication authentication) throws IOException {
        assertThreadContextContainsAuthentication(authentication, true);
    }

    void assertThreadContextContainsAuthentication(Authentication authentication, boolean sign) throws IOException {
        Authentication contextAuth = threadContext.getTransient(Authentication.AUTHENTICATION_KEY);
        assertThat(contextAuth, notNullValue());
        assertThat(contextAuth, is(authentication));
        if (sign) {
            assertThat(threadContext.getHeader(Authentication.AUTHENTICATION_KEY), equalTo((Object) "_signed_auth"));
        } else {
            assertThat(threadContext.getHeader(Authentication.AUTHENTICATION_KEY), equalTo((Object) authentication.encode()));
        }
    }
}
