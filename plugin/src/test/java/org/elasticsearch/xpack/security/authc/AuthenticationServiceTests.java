/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.security.authc.AuthenticationService.Authenticator;
import org.elasticsearch.xpack.security.authc.Realm.Factory;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.authc.support.SecuredString;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.security.user.User;
import org.junit.Before;

import static org.elasticsearch.test.SecurityTestsUtils.assertAuthenticationException;
import static org.elasticsearch.xpack.security.support.Exceptions.authenticationError;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;


/**
 * Unit tests for the {@link AuthenticationService}
 */
public class AuthenticationServiceTests extends ESTestCase {

    private AuthenticationService service;
    private TransportMessage message;
    private RestRequest restRequest;
    private Realms realms;
    private Realm firstRealm;
    private Realm secondRealm;
    private AuditTrailService auditTrail;
    private AuthenticationToken token;
    private ThreadPool threadPool;
    private ThreadContext threadContext;

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
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.allowedRealmType()).thenReturn(XPackLicenseState.AllowedRealmType.ALL);
        when(licenseState.isAuthAllowed()).thenReturn(true);
        realms = new TestRealms(Settings.EMPTY, new Environment(settings), Collections.<String, Realm.Factory>emptyMap(),
            licenseState, mock(ReservedRealm.class), Arrays.asList(firstRealm, secondRealm), Collections.singletonList(firstRealm));

        auditTrail = mock(AuditTrailService.class);
        threadPool = mock(ThreadPool.class);
        threadContext = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        service = new AuthenticationService(settings, realms, auditTrail,
                new DefaultAuthenticationFailureHandler(), threadPool, new AnonymousUser(settings));
    }

    @SuppressWarnings("unchecked")
    public void testTokenFirstMissingSecondFound() throws Exception {
        when(firstRealm.token(threadContext)).thenReturn(null);
        when(secondRealm.token(threadContext)).thenReturn(token);

        PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        Authenticator authenticator = service.createAuthenticator("_action", message, null, future);
        authenticator.extractToken((result) -> {
            assertThat(result, notNullValue());
            assertThat(result, is(token));
            verifyZeroInteractions(auditTrail);
        });
    }

    public void testTokenMissing() throws Exception {
        PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        Authenticator authenticator = service.createAuthenticator("_action", message, null, future);
        authenticator.extractToken((token) -> {
            assertThat(token, nullValue());
            authenticator.handleNullToken();
        });

        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> future.actionGet());
        assertThat(e.getMessage(), containsString("missing authentication token"));
        verify(auditTrail).anonymousAccessDenied("_action", message);
        verifyNoMoreInteractions(auditTrail);
    }

    @SuppressWarnings("unchecked")
    public void testAuthenticateBothSupportSecondSucceeds() throws Exception {
        User user = new User("_username", "r1");
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, null);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, user);
        if (randomBoolean()) {
            when(firstRealm.token(threadContext)).thenReturn(token);
        } else {
            when(secondRealm.token(threadContext)).thenReturn(token);
        }

        Authentication result = authenticateBlocking("_action", message, null);
        assertThat(result, notNullValue());
        assertThat(result.getUser(), is(user));
        assertThat(result.getLookedUpBy(), is(nullValue()));
        assertThat(result.getAuthenticatedBy(), is(notNullValue())); // TODO implement equals
        verify(auditTrail).authenticationFailed(firstRealm.name(), token, "_action", message);
        assertThreadContextContainsAuthentication(result);
    }

    public void testAuthenticateFirstNotSupportingSecondSucceeds() throws Exception {
        User user = new User("_username", "r1");
        when(firstRealm.supports(token)).thenReturn(false);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, user);
        when(secondRealm.token(threadContext)).thenReturn(token);

        Authentication result = authenticateBlocking("_action", message, null);
        assertThat(result, notNullValue());
        assertThat(result.getUser(), is(user));
        verify(auditTrail).authenticationSuccess(secondRealm.name(), user, "_action", message);
        verifyNoMoreInteractions(auditTrail);
        verify(firstRealm, never()).authenticate(eq(token), any(ActionListener.class));
        assertThreadContextContainsAuthentication(result);
    }

    public void testAuthenticateCached() throws Exception {
        final Authentication authentication = new Authentication(new User("_username", "r1"), new RealmRef("test", "cached", "foo"), null);
        authentication.writeToContext(threadContext);

        Authentication result = authenticateBlocking("_action", message, null);

        assertThat(result, notNullValue());
        assertThat(result, is(authentication));
        verifyZeroInteractions(auditTrail);
        verifyZeroInteractions(firstRealm);
        verifyZeroInteractions(secondRealm);
    }

    public void testAuthenticateNonExistentRestRequestUserThrowsAuthenticationException() throws Exception {
        when(firstRealm.token(threadContext)).thenReturn(new UsernamePasswordToken("idonotexist",
                new SecuredString("passwd".toCharArray())));
        try {
            authenticateBlocking(restRequest);
            fail("Authentication was successful but should not");
        } catch (ElasticsearchSecurityException e) {
            assertAuthenticationException(e, containsString("unable to authenticate user [idonotexist] for REST request [/]"));
        }
    }

    public void testTokenRestMissing() throws Exception {
        when(firstRealm.token(threadContext)).thenReturn(null);
        when(secondRealm.token(threadContext)).thenReturn(null);

        Authenticator authenticator = service.createAuthenticator(restRequest, mock(ActionListener.class));
        authenticator.extractToken((token) -> {
            assertThat(token, nullValue());
        });
    }

    public void authenticationInContextAndHeader() throws Exception {
        User user = new User("_username", "r1");
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user);

        Authentication result = authenticateBlocking("_action", message, null);

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
            authenticateBlocking("_action", message, null);
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
            authenticateBlocking(restRequest);
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

        Authentication result = authenticateBlocking("_action", message, user1);
        assertThat(result, notNullValue());
        assertThat(result.getUser(), sameInstance(user1));
        assertThreadContextContainsAuthentication(result);
    }

    public void testAuthenticateTransportDisabledUser() throws Exception {
        User user = new User("username", new String[] { "r1", "r2" }, null, null, null, false);
        User fallback = randomBoolean() ? SystemUser.INSTANCE : null;
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user);

        ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking("_action", message, fallback));
        verify(auditTrail).authenticationFailed(token, "_action", message);
        verifyNoMoreInteractions(auditTrail);
        assertAuthenticationException(e);
    }

    public void testAuthenticateRestDisabledUser() throws Exception {
        User user = new User("username", new String[] { "r1", "r2" }, null, null, null, false);
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user);

        ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking(restRequest));
        verify(auditTrail).authenticationFailed(token, restRequest);
        verifyNoMoreInteractions(auditTrail);
        assertAuthenticationException(e);
    }

    public void testAuthenticateTransportSuccess() throws Exception {
        User user = new User("username", "r1", "r2");
        User fallback = randomBoolean() ? SystemUser.INSTANCE : null;
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user);

        Authentication result = authenticateBlocking("_action", message, fallback);
        assertThat(result, notNullValue());
        assertThat(result.getUser(), sameInstance(user));
        assertThreadContextContainsAuthentication(result);
        verify(auditTrail).authenticationSuccess(firstRealm.name(), user, "_action", message);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testAuthenticateRestSuccess() throws Exception {
        User user1 = new User("username", "r1", "r2");
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user1);
        Authentication result = authenticateBlocking(restRequest);
        assertThat(result, notNullValue());
        assertThat(result.getUser(), sameInstance(user1));
        assertThreadContextContainsAuthentication(result);
        verify(auditTrail).authenticationSuccess(firstRealm.name(), user1, restRequest);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testAutheticateTransportContextAndHeader() throws Exception {
        User user1 = new User("username", "r1", "r2");
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user1);
        Authentication authentication = authenticateBlocking("_action", message, SystemUser.INSTANCE);
        assertThat(authentication, notNullValue());
        assertThat(authentication.getUser(), sameInstance(user1));
        assertThreadContextContainsAuthentication(authentication);
        reset(firstRealm);

        // checking authentication from the context
        InternalMessage message1 = new InternalMessage();
        ThreadContext threadContext1 = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext1);
        service = new AuthenticationService(Settings.EMPTY, realms, auditTrail,
                new DefaultAuthenticationFailureHandler(), threadPool, new AnonymousUser(Settings.EMPTY));

        threadContext1.putTransient(Authentication.AUTHENTICATION_KEY, threadContext.getTransient(Authentication.AUTHENTICATION_KEY));
        threadContext1.putHeader(Authentication.AUTHENTICATION_KEY, threadContext.getHeader(Authentication.AUTHENTICATION_KEY));
        Authentication ctxAuth = authenticateBlocking("_action", message1, SystemUser.INSTANCE);
        assertThat(ctxAuth, sameInstance(authentication));
        verifyZeroInteractions(firstRealm);
        reset(firstRealm);

        // checking authentication from the user header
        threadContext1 = new ThreadContext(Settings.EMPTY);
        when(threadPool.getThreadContext()).thenReturn(threadContext1);
        service = new AuthenticationService(Settings.EMPTY, realms, auditTrail,
                new DefaultAuthenticationFailureHandler(), threadPool, new AnonymousUser(Settings.EMPTY));
        threadContext1.putHeader(Authentication.AUTHENTICATION_KEY, threadContext.getHeader(Authentication.AUTHENTICATION_KEY));

        BytesStreamOutput output = new BytesStreamOutput();
        threadContext1.writeTo(output);
        StreamInput input = output.bytes().streamInput();
        threadContext1 = new ThreadContext(Settings.EMPTY);
        threadContext1.readHeaders(input);

        when(threadPool.getThreadContext()).thenReturn(threadContext1);
        service = new AuthenticationService(Settings.EMPTY, realms, auditTrail,
                new DefaultAuthenticationFailureHandler(), threadPool, new AnonymousUser(Settings.EMPTY));
        Authentication result = authenticateBlocking("_action", new InternalMessage(), SystemUser.INSTANCE);
        assertThat(result, notNullValue());
        assertThat(result.getUser(), equalTo(user1));
        verifyZeroInteractions(firstRealm);
    }

    public void testAuthenticateTamperedUser() throws Exception {
        InternalMessage message = new InternalMessage();
        threadContext.putHeader(Authentication.AUTHENTICATION_KEY, "_signed_auth");

        try {
            authenticateBlocking("_action", message, randomBoolean() ? SystemUser.INSTANCE : null);
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
        assertThat(threadContext.getHeader(Authentication.AUTHENTICATION_KEY), equalTo((Object) authentication.encode()));
    }

    public void testAttachIfMissingExists() throws Exception {
        Authentication authentication = new Authentication(new User("username", "r1", "r2"), new RealmRef("test", "test", "foo"), null);
        threadContext.putTransient(Authentication.AUTHENTICATION_KEY, authentication);
        threadContext.putHeader(Authentication.AUTHENTICATION_KEY, authentication.encode());
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
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        service = new AuthenticationService(settings, realms, auditTrail, new DefaultAuthenticationFailureHandler(),
                threadPool, anonymousUser);
        RestRequest request = new FakeRestRequest();

        Authentication result = authenticateBlocking(request);

        assertThat(result, notNullValue());
        assertThat(result.getUser(), sameInstance((Object) anonymousUser));
        assertThreadContextContainsAuthentication(result);
        verify(auditTrail).authenticationSuccess("__anonymous", new AnonymousUser(settings), request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testAnonymousUserTransportNoDefaultUser() throws Exception {
        Settings settings = Settings.builder()
                .putArray(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3")
                .build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        service = new AuthenticationService(settings, realms, auditTrail,
                new DefaultAuthenticationFailureHandler(), threadPool, anonymousUser);
        InternalMessage message = new InternalMessage();

        Authentication result = authenticateBlocking("_action", message, null);
        assertThat(result, notNullValue());
        assertThat(result.getUser(), sameInstance(anonymousUser));
        assertThreadContextContainsAuthentication(result);
    }

    public void testAnonymousUserTransportWithDefaultUser() throws Exception {
        Settings settings = Settings.builder()
                .putArray(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3")
                .build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        service = new AuthenticationService(settings, realms, auditTrail,
                new DefaultAuthenticationFailureHandler(), threadPool, anonymousUser);

        InternalMessage message = new InternalMessage();

        Authentication result = authenticateBlocking("_action", message, SystemUser.INSTANCE);
        assertThat(result, notNullValue());
        assertThat(result.getUser(), sameInstance(SystemUser.INSTANCE));
        assertThreadContextContainsAuthentication(result);
    }

    public void testRealmTokenThrowingException() throws Exception {
        when(firstRealm.token(threadContext)).thenThrow(authenticationError("realm doesn't like tokens"));
        try {
            authenticateBlocking("_action", message, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like tokens"));
            verify(auditTrail).authenticationFailed("_action", message);
        }
    }

    public void testRealmTokenThrowingExceptionRest() throws Exception {
        when(firstRealm.token(threadContext)).thenThrow(authenticationError("realm doesn't like tokens"));
        try {
            authenticateBlocking(restRequest);
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
            authenticateBlocking("_action", message, null);
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
            authenticateBlocking(restRequest);
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
        doThrow(authenticationError("realm doesn't like authenticate"))
            .when(secondRealm).authenticate(eq(token), any(ActionListener.class));
        try {
            authenticateBlocking("_action", message, null);
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
        doThrow(authenticationError("realm doesn't like authenticate"))
                .when(secondRealm).authenticate(eq(token), any(ActionListener.class));
        try {
            authenticateBlocking(restRequest);
            fail("exception should bubble out");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.getMessage(), is("realm doesn't like authenticate"));
            verify(auditTrail).authenticationFailed(token, restRequest);
        }
    }

    public void testRealmLookupThrowingException() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        threadContext.putHeader(AuthenticationService.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, new User("lookup user", new String[]{"user"}));
        mockRealmLookupReturnsNull(firstRealm, "run_as");
        doThrow(authenticationError("realm doesn't want to lookup"))
            .when(secondRealm).lookupUser(eq("run_as"), any(ActionListener.class));

        try {
            authenticateBlocking("_action", message, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't want to lookup"));
            verify(auditTrail).authenticationFailed(token, "_action", message);
        }
    }

    public void testRealmLookupThrowingExceptionRest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        threadContext.putHeader(AuthenticationService.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, new User("lookup user", new String[]{"user"}));
        mockRealmLookupReturnsNull(firstRealm, "run_as");
        doThrow(authenticationError("realm doesn't want to " + "lookup"))
                .when(secondRealm).lookupUser(eq("run_as"), any(ActionListener.class));

        try {
            authenticateBlocking(restRequest);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't want to lookup"));
            verify(auditTrail).authenticationFailed(token, restRequest);
        }
    }

    public void testRunAsLookupSameRealm() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        threadContext.putHeader(AuthenticationService.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        final User user = new User("lookup user", new String[]{"user"}, "lookup user", "lookup@foo.foo",
                Collections.singletonMap("foo", "bar"), true);
        mockAuthenticate(secondRealm, token, user);
        mockRealmLookupReturnsNull(firstRealm, "run_as");
        doAnswer((i) -> {
            ActionListener listener = (ActionListener) i.getArguments()[1];
            listener.onResponse(new User("looked up user", new String[]{"some role"}));
            return null;
        }).when(secondRealm).lookupUser(eq("run_as"), any(ActionListener.class));

        Authentication result;
        if (randomBoolean()) {
            result = authenticateBlocking("_action", message, null);
        } else {
            result = authenticateBlocking(restRequest);
        }
        assertThat(result, notNullValue());
        User authenticated = result.getUser();

        assertThat(SystemUser.is(authenticated), is(false));
        assertThat(authenticated.runAs(), is(notNullValue()));
        assertThat(authenticated.principal(), is("lookup user"));
        assertThat(authenticated.roles(), arrayContaining("user"));
        assertEquals(user.metadata(), authenticated.metadata());
        assertEquals(user.email(), authenticated.email());
        assertEquals(user.enabled(), authenticated.enabled());
        assertEquals(user.fullName(), authenticated.fullName());

        assertThat(authenticated.runAs().principal(), is("looked up user"));
        assertThat(authenticated.runAs().roles(), arrayContaining("some role"));
        assertThreadContextContainsAuthentication(result);
    }

    public void testRunAsLookupDifferentRealm() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        threadContext.putHeader(AuthenticationService.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, new User("lookup user", new String[]{"user"}));
        doAnswer((i) -> {
            ActionListener listener = (ActionListener) i.getArguments()[1];
            listener.onResponse(new User("looked up user", new String[]{"some role"}));
            return null;
        }).when(firstRealm).lookupUser(eq("run_as"), any(ActionListener.class));

        Authentication result;
        if (randomBoolean()) {
            result = authenticateBlocking("_action", message, null);
        } else {
            result = authenticateBlocking(restRequest);
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
        threadContext.putHeader(AuthenticationService.RUN_AS_USER_HEADER, "");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, user);

        try {
            authenticateBlocking(restRequest);
            fail("exception should be thrown");
        } catch (ElasticsearchException e) {
            verify(auditTrail).runAsDenied(any(User.class), eq(restRequest));
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testRunAsWithEmptyRunAsUsername() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        User user = new User("lookup user", new String[]{"user"});
        threadContext.putHeader(AuthenticationService.RUN_AS_USER_HEADER, "");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, user);

        try {
            authenticateBlocking("_action", message, null);
            fail("exception should be thrown");
        } catch (ElasticsearchException e) {
            verify(auditTrail).runAsDenied(any(User.class), eq("_action"), eq(message));
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testAuthenticateTransportDisabledRunAsUser() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        threadContext.putHeader(AuthenticationService.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, new User("lookup user", new String[]{"user"}));
        mockRealmLookupReturnsNull(firstRealm, "run_as");
        doAnswer((i) -> {
            ActionListener listener = (ActionListener) i.getArguments()[1];
            listener.onResponse(new User("looked up user", new String[]{"some role"}, null, null, null, false));
            return null;
        }).when(secondRealm).lookupUser(eq("run_as"), any(ActionListener.class));
        User fallback = randomBoolean() ? SystemUser.INSTANCE : null;
        ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking("_action", message, fallback));
        verify(auditTrail).authenticationFailed(token, "_action", message);
        verifyNoMoreInteractions(auditTrail);
        assertAuthenticationException(e);
    }

    public void testAuthenticateRestDisabledRunAsUser() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        threadContext.putHeader(AuthenticationService.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, new User("lookup user", new String[]{"user"}));
        mockRealmLookupReturnsNull(firstRealm, "run_as");
        doAnswer((i) -> {
            ActionListener listener = (ActionListener) i.getArguments()[1];
            listener.onResponse(new User("looked up user", new String[]{"some role"}, null, null, null, false));
            return null;
        }).when(secondRealm).lookupUser(eq("run_as"), any(ActionListener.class));

        ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking(restRequest));
        verify(auditTrail).authenticationFailed(token, restRequest);
        verifyNoMoreInteractions(auditTrail);
        assertAuthenticationException(e);
    }

    private static class InternalMessage extends TransportMessage {
    }

    void assertThreadContextContainsAuthentication(Authentication authentication) throws IOException {
        Authentication contextAuth = threadContext.getTransient(Authentication.AUTHENTICATION_KEY);
        assertThat(contextAuth, notNullValue());
        assertThat(contextAuth, is(authentication));
        assertThat(threadContext.getHeader(Authentication.AUTHENTICATION_KEY), equalTo((Object) authentication.encode()));
    }

    private void mockAuthenticate(Realm realm, AuthenticationToken token, User user) {
        doAnswer((i) -> {
            ActionListener listener = (ActionListener) i.getArguments()[1];
            listener.onResponse(user);
            return null;
        }).when(realm).authenticate(eq(token), any(ActionListener.class));
    }

    private Authentication authenticateBlocking(RestRequest restRequest) {
        PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        service.authenticate(restRequest, future);
        return future.actionGet();
    }

    private Authentication authenticateBlocking(String action, TransportMessage message, User fallbackUser) {
        PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        service.authenticate(action, message, fallbackUser, future);
        return future.actionGet();
    }

    private static void mockRealmLookupReturnsNull(Realm realm, String username) {
        doAnswer((i) -> {
            ActionListener listener = (ActionListener) i.getArguments()[1];
            listener.onResponse(null);
            return null;
        }).when(realm).lookupUser(eq(username), any(ActionListener.class));
    }

    static class TestRealms extends Realms {

        TestRealms(Settings settings, Environment env, Map<String, Factory> factories, XPackLicenseState licenseState,
                   ReservedRealm reservedRealm, List<Realm> realms, List<Realm> internalRealms) throws Exception {
            super(settings, env, factories, licenseState, reservedRealm);
            this.realms = realms;
            this.internalRealmsOnly = internalRealms;
        }
    }
}
