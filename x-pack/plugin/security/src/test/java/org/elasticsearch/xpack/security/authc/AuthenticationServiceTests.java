/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.DefaultAuthenticationFailureHandler;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.Realm.Factory;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.AuthenticationService.Authenticator;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.elasticsearch.test.SecurityTestsUtils.assertAuthenticationException;
import static org.elasticsearch.xpack.core.security.support.Exceptions.authenticationError;
import static org.elasticsearch.xpack.security.authc.TokenServiceTests.mockGetTokenFromId;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
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
    private TokenService tokenService;
    private SecurityIndexManager securityIndex;
    private Client client;
    private InetSocketAddress remoteAddress;

    @Before
    @SuppressForbidden(reason = "Allow accessing localhost")
    public void init() throws Exception {
        token = mock(AuthenticationToken.class);
        message = new InternalMessage();
        remoteAddress = new InetSocketAddress(InetAddress.getLocalHost(), 100);
        message.remoteAddress(new TransportAddress(remoteAddress));
        restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withRemoteAddress(remoteAddress).build();
        threadContext = new ThreadContext(Settings.EMPTY);

        firstRealm = mock(Realm.class);
        when(firstRealm.type()).thenReturn("file");
        when(firstRealm.name()).thenReturn("file_realm");
        secondRealm = mock(Realm.class);
        when(secondRealm.type()).thenReturn("second");
        when(secondRealm.name()).thenReturn("second_realm");
        Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put("node.name", "authc_test")
                .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
                .build();
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.allowedRealmType()).thenReturn(XPackLicenseState.AllowedRealmType.ALL);
        when(licenseState.isAuthAllowed()).thenReturn(true);
        realms = new TestRealms(Settings.EMPTY, TestEnvironment.newEnvironment(settings), Collections.<String, Realm.Factory>emptyMap(),
                licenseState, threadContext, mock(ReservedRealm.class), Arrays.asList(firstRealm, secondRealm),
                Collections.singletonList(firstRealm));

        auditTrail = mock(AuditTrailService.class);
        client = mock(Client.class);
        threadPool = new ThreadPool(settings,
                new FixedExecutorBuilder(settings, TokenService.THREAD_POOL_NAME, 1, 1000, "xpack.security.authc.token.thread_pool"));
        threadContext = threadPool.getThreadContext();
        when(client.threadPool()).thenReturn(threadPool);
        when(client.settings()).thenReturn(settings);
        when(client.prepareIndex(any(String.class), any(String.class), any(String.class)))
                .thenReturn(new IndexRequestBuilder(client, IndexAction.INSTANCE));
        when(client.prepareUpdate(any(String.class), any(String.class), any(String.class)))
                .thenReturn(new UpdateRequestBuilder(client, UpdateAction.INSTANCE));
        doAnswer(invocationOnMock -> {
            ActionListener<IndexResponse> responseActionListener = (ActionListener<IndexResponse>) invocationOnMock.getArguments()[2];
            responseActionListener.onResponse(new IndexResponse());
            return null;
        }).when(client).execute(eq(IndexAction.INSTANCE), any(IndexRequest.class), any(ActionListener.class));
        doAnswer(invocationOnMock -> {
            GetRequestBuilder builder = new GetRequestBuilder(client, GetAction.INSTANCE);
            builder.setIndex((String) invocationOnMock.getArguments()[0])
                    .setType((String) invocationOnMock.getArguments()[1])
                    .setId((String) invocationOnMock.getArguments()[2]);
            return builder;
        }).when(client).prepareGet(anyString(), anyString(), anyString());
        securityIndex = mock(SecurityIndexManager.class);
        doAnswer(invocationOnMock -> {
            Runnable runnable = (Runnable) invocationOnMock.getArguments()[1];
            runnable.run();
            return null;
        }).when(securityIndex).prepareIndexIfNeededThenExecute(any(Consumer.class), any(Runnable.class));
        doAnswer(invocationOnMock -> {
            Runnable runnable = (Runnable) invocationOnMock.getArguments()[1];
            runnable.run();
            return null;
        }).when(securityIndex).checkIndexVersionThenExecute(any(Consumer.class), any(Runnable.class));
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        tokenService = new TokenService(settings, Clock.systemUTC(), client, securityIndex, clusterService);
        service = new AuthenticationService(settings, realms, auditTrail,
                new DefaultAuthenticationFailureHandler(Collections.emptyMap()), threadPool, new AnonymousUser(settings), tokenService);
    }

    @After
    public void shutdownThreadpool() throws InterruptedException {
        if (threadPool != null) {
            terminate(threadPool);
        }
    }

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
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
        PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        Authenticator authenticator = service.createAuthenticator("_action", message, null, future);
        authenticator.extractToken((token) -> {
            assertThat(token, nullValue());
            authenticator.handleNullToken();
        });

        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, () -> future.actionGet());
        assertThat(e.getMessage(), containsString("missing authentication credentials"));
        verify(auditTrail).anonymousAccessDenied(reqId, "_action", message);
        verifyNoMoreInteractions(auditTrail);
    }

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
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);

        final AtomicBoolean completed = new AtomicBoolean(false);
        service.authenticate("_action", message, (User)null, ActionListener.wrap(result -> {
            assertThat(result, notNullValue());
            assertThat(result.getUser(), is(user));
            assertThat(result.getLookedUpBy(), is(nullValue()));
            assertThat(result.getAuthenticatedBy(), is(notNullValue())); // TODO implement equals
            assertThreadContextContainsAuthentication(result);
            setCompletedToTrue(completed);
        }, this::logAndFail));
        assertTrue(completed.get());
        verify(auditTrail).authenticationFailed(reqId, firstRealm.name(), token, "_action", message);
    }

    public void testAuthenticateFirstNotSupportingSecondSucceeds() throws Exception {
        User user = new User("_username", "r1");
        when(firstRealm.supports(token)).thenReturn(false);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, user);
        when(secondRealm.token(threadContext)).thenReturn(token);
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);

        final AtomicBoolean completed = new AtomicBoolean(false);
        service.authenticate("_action", message, (User)null, ActionListener.wrap(result -> {
            assertThat(result, notNullValue());
            assertThat(result.getUser(), is(user));
            assertThreadContextContainsAuthentication(result);
            setCompletedToTrue(completed);
        }, this::logAndFail));
        verify(auditTrail).authenticationSuccess(reqId, secondRealm.name(), user, "_action", message);
        verifyNoMoreInteractions(auditTrail);
        verify(firstRealm, never()).authenticate(eq(token), any(ActionListener.class));
        assertTrue(completed.get());
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
                new SecureString("passwd".toCharArray())));
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

        String userStr = threadContext.getHeader(AuthenticationField.AUTHENTICATION_KEY);
        assertThat(userStr, notNullValue());
        assertThat(userStr, equalTo("_signed_auth"));

        Authentication ctxAuth = threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY);
        assertThat(ctxAuth, is(result));
    }

    public void testAuthenticateTransportAnonymous() throws Exception {
        when(firstRealm.token(threadContext)).thenReturn(null);
        when(secondRealm.token(threadContext)).thenReturn(null);
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
        try {
            authenticateBlocking("_action", message, null);
            fail("expected an authentication exception when trying to authenticate an anonymous message");
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertAuthenticationException(e);
        }
        verify(auditTrail).anonymousAccessDenied(reqId, "_action", message);
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
        String reqId = expectAuditRequestId();
        verify(auditTrail).anonymousAccessDenied(reqId, restRequest);
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
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
        User user = new User("username", new String[] { "r1", "r2" }, null, null, null, false);
        User fallback = randomBoolean() ? SystemUser.INSTANCE : null;
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user);

        ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking("_action", message, fallback));
        verify(auditTrail).authenticationFailed(reqId, token, "_action", message);
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
        String reqId = expectAuditRequestId();
        verify(auditTrail).authenticationFailed(reqId, token, restRequest);
        verifyNoMoreInteractions(auditTrail);
        assertAuthenticationException(e);
    }

    public void testAuthenticateTransportSuccess() throws Exception {
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
        User user = new User("username", "r1", "r2");
        User fallback = randomBoolean() ? SystemUser.INSTANCE : null;
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user);

        final AtomicBoolean completed = new AtomicBoolean(false);
        service.authenticate("_action", message, fallback, ActionListener.wrap(result -> {
            assertThat(result, notNullValue());
            assertThat(result.getUser(), sameInstance(user));
            assertThreadContextContainsAuthentication(result);
            setCompletedToTrue(completed);
        }, this::logAndFail));

        verify(auditTrail).authenticationSuccess(reqId, firstRealm.name(), user, "_action", message);
        verifyNoMoreInteractions(auditTrail);
        assertTrue(completed.get());
    }

    public void testAuthenticateRestSuccess() throws Exception {
        User user1 = new User("username", "r1", "r2");
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user1);
        // this call does not actually go async
        final AtomicBoolean completed = new AtomicBoolean(false);
        service.authenticate(restRequest, ActionListener.wrap(authentication -> {
            assertThat(authentication, notNullValue());
            assertThat(authentication.getUser(), sameInstance(user1));
            assertThreadContextContainsAuthentication(authentication);
            setCompletedToTrue(completed);
        }, this::logAndFail));
        String reqId = expectAuditRequestId();
        verify(auditTrail).authenticationSuccess(reqId, firstRealm.name(), user1, restRequest);
        verifyNoMoreInteractions(auditTrail);
        assertTrue(completed.get());
    }

    public void testAutheticateTransportContextAndHeader() throws Exception {
        User user1 = new User("username", "r1", "r2");
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user1);
        final AtomicBoolean completed = new AtomicBoolean(false);
        final SetOnce<Authentication> authRef = new SetOnce<>();
        final SetOnce<String> authHeaderRef = new SetOnce<>();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            service.authenticate("_action", message, SystemUser.INSTANCE, ActionListener.wrap(authentication -> {

                assertThat(authentication, notNullValue());
                assertThat(authentication.getUser(), sameInstance(user1));
                assertThreadContextContainsAuthentication(authentication);
                authRef.set(authentication);
                authHeaderRef.set(threadContext.getHeader(AuthenticationField.AUTHENTICATION_KEY));
                setCompletedToTrue(completed);
            }, this::logAndFail));
        }
        assertTrue(completed.compareAndSet(true, false));
        reset(firstRealm);

        // checking authentication from the context
        InternalMessage message1 = new InternalMessage();
        ThreadPool threadPool1 = new TestThreadPool("testAutheticateTransportContextAndHeader1");
        try {
            ThreadContext threadContext1 = threadPool1.getThreadContext();
            service = new AuthenticationService(Settings.EMPTY, realms, auditTrail,
                new DefaultAuthenticationFailureHandler(Collections.emptyMap()), threadPool1, new AnonymousUser(Settings.EMPTY),
                tokenService);

            threadContext1.putTransient(AuthenticationField.AUTHENTICATION_KEY, authRef.get());
            threadContext1.putHeader(AuthenticationField.AUTHENTICATION_KEY, authHeaderRef.get());
            service.authenticate("_action", message1, SystemUser.INSTANCE, ActionListener.wrap(ctxAuth -> {
                assertThat(ctxAuth, sameInstance(authRef.get()));
                assertThat(threadContext1.getHeader(AuthenticationField.AUTHENTICATION_KEY), sameInstance(authHeaderRef.get()));
                setCompletedToTrue(completed);
            }, this::logAndFail));
            assertTrue(completed.compareAndSet(true, false));
            verifyZeroInteractions(firstRealm);
            reset(firstRealm);
        } finally {
            terminate(threadPool1);
        }

        // checking authentication from the user header
        ThreadPool threadPool2 = new TestThreadPool("testAutheticateTransportContextAndHeader2");
        try {
            ThreadContext threadContext2 = threadPool2.getThreadContext();
            final String header;
            try (ThreadContext.StoredContext ignore = threadContext2.stashContext()) {
                service = new AuthenticationService(Settings.EMPTY, realms, auditTrail,
                    new DefaultAuthenticationFailureHandler(Collections.emptyMap()), threadPool2, new AnonymousUser(Settings.EMPTY),
                    tokenService);
                threadContext2.putHeader(AuthenticationField.AUTHENTICATION_KEY, authHeaderRef.get());

                BytesStreamOutput output = new BytesStreamOutput();
                threadContext2.writeTo(output);
                StreamInput input = output.bytes().streamInput();
                threadContext2 = new ThreadContext(Settings.EMPTY);
                threadContext2.readHeaders(input);
                header = threadContext2.getHeader(AuthenticationField.AUTHENTICATION_KEY);
            }

            threadPool2.getThreadContext().putHeader(AuthenticationField.AUTHENTICATION_KEY, header);
            service = new AuthenticationService(Settings.EMPTY, realms, auditTrail,
                new DefaultAuthenticationFailureHandler(Collections.emptyMap()), threadPool2, new AnonymousUser(Settings.EMPTY),
                tokenService);
            service.authenticate("_action", new InternalMessage(), SystemUser.INSTANCE, ActionListener.wrap(result -> {
                assertThat(result, notNullValue());
                assertThat(result.getUser(), equalTo(user1));
                setCompletedToTrue(completed);
            }, this::logAndFail));
            assertTrue(completed.get());
            verifyZeroInteractions(firstRealm);
        } finally {
            terminate(threadPool2);
        }
    }

    public void testAuthenticateTamperedUser() throws Exception {
        InternalMessage message = new InternalMessage();
        threadContext.putHeader(AuthenticationField.AUTHENTICATION_KEY, "_signed_auth");
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);

        try {
            authenticateBlocking("_action", message, randomBoolean() ? SystemUser.INSTANCE : null);
        } catch (Exception e) {
            //expected
            verify(auditTrail).tamperedRequest(reqId, "_action", message);
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testAnonymousUserRest() throws Exception {
        String username = randomBoolean() ? AnonymousUser.DEFAULT_ANONYMOUS_USERNAME : "user1";
        Settings.Builder builder = Settings.builder()
                .putList(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3");
        if (username.equals(AnonymousUser.DEFAULT_ANONYMOUS_USERNAME) == false) {
            builder.put(AnonymousUser.USERNAME_SETTING.getKey(), username);
        }
        Settings settings = builder.build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        service = new AuthenticationService(settings, realms, auditTrail, new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool, anonymousUser, tokenService);
        RestRequest request = new FakeRestRequest();

        Authentication result = authenticateBlocking(request);

        assertThat(result, notNullValue());
        assertThat(result.getUser(), sameInstance((Object) anonymousUser));
        assertThreadContextContainsAuthentication(result);
        String reqId = expectAuditRequestId();
        verify(auditTrail).authenticationSuccess(reqId, "__anonymous", new AnonymousUser(settings), request);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testAnonymousUserTransportNoDefaultUser() throws Exception {
        Settings settings = Settings.builder()
                .putList(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3")
                .build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        service = new AuthenticationService(settings, realms, auditTrail, new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool, anonymousUser, tokenService);
        InternalMessage message = new InternalMessage();

        Authentication result = authenticateBlocking("_action", message, null);
        assertThat(result, notNullValue());
        assertThat(result.getUser(), sameInstance(anonymousUser));
        assertThreadContextContainsAuthentication(result);
    }

    public void testAnonymousUserTransportWithDefaultUser() throws Exception {
        Settings settings = Settings.builder()
                .putList(AnonymousUser.ROLES_SETTING.getKey(), "r1", "r2", "r3")
                .build();
        final AnonymousUser anonymousUser = new AnonymousUser(settings);
        service = new AuthenticationService(settings, realms, auditTrail, new DefaultAuthenticationFailureHandler(Collections.emptyMap()),
            threadPool, anonymousUser, tokenService);

        InternalMessage message = new InternalMessage();

        Authentication result = authenticateBlocking("_action", message, SystemUser.INSTANCE);
        assertThat(result, notNullValue());
        assertThat(result.getUser(), sameInstance(SystemUser.INSTANCE));
        assertThreadContextContainsAuthentication(result);
    }

    public void testRealmTokenThrowingException() throws Exception {
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
        when(firstRealm.token(threadContext)).thenThrow(authenticationError("realm doesn't like tokens"));
        try {
            authenticateBlocking("_action", message, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like tokens"));
            verify(auditTrail).authenticationFailed(reqId, "_action", message);
        }
    }

    public void testRealmTokenThrowingExceptionRest() throws Exception {
        when(firstRealm.token(threadContext)).thenThrow(authenticationError("realm doesn't like tokens"));
        try {
            authenticateBlocking(restRequest);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like tokens"));
            String reqId = expectAuditRequestId();
            verify(auditTrail).authenticationFailed(reqId, restRequest);
        }
    }

    public void testRealmSupportsMethodThrowingException() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenThrow(authenticationError("realm doesn't like supports"));
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
        try {
            authenticateBlocking("_action", message, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like supports"));
            verify(auditTrail).authenticationFailed(reqId, token, "_action", message);
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
            String reqId = expectAuditRequestId();
            verify(auditTrail).authenticationFailed(reqId, token, restRequest);
        }
    }

    public void testRealmAuthenticateTerminatingAuthenticationProcess() throws Exception {
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
        final AuthenticationToken token = mock(AuthenticationToken.class);
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        final boolean terminateWithNoException = rarely();
        final boolean throwElasticsearchSecurityException = (terminateWithNoException == false) && randomBoolean();
        final boolean withAuthenticateHeader = throwElasticsearchSecurityException && randomBoolean();
        Exception throwE = new Exception("general authentication error");
        final String basicScheme = "Basic realm=\"" + XPackField.SECURITY + "\" charset=\"UTF-8\"";
        String selectedScheme = randomFrom(basicScheme, "Negotiate IOJoj");
        if (throwElasticsearchSecurityException) {
            throwE = new ElasticsearchSecurityException("authentication error", RestStatus.UNAUTHORIZED);
            if (withAuthenticateHeader) {
                ((ElasticsearchSecurityException) throwE).addHeader("WWW-Authenticate", selectedScheme);
            }
        }
        mockAuthenticate(secondRealm, token, (terminateWithNoException) ? null : throwE, true);

        ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking("_action", message, null));
        if (terminateWithNoException) {
            assertThat(e.getMessage(), is("terminate authc process"));
            assertThat(e.getHeader("WWW-Authenticate"), contains(basicScheme));
        } else {
            if (throwElasticsearchSecurityException) {
                assertThat(e.getMessage(), is("authentication error"));
                if (withAuthenticateHeader) {
                    assertThat(e.getHeader("WWW-Authenticate"), contains(selectedScheme));
                } else {
                    assertThat(e.getHeader("WWW-Authenticate"), contains(basicScheme));
                }
            } else {
                assertThat(e.getMessage(), is("error attempting to authenticate request"));
                assertThat(e.getHeader("WWW-Authenticate"), contains(basicScheme));
            }
        }
        verify(auditTrail).authenticationFailed(reqId, secondRealm.name(), token, "_action", message);
        verify(auditTrail).authenticationFailed(reqId, token, "_action", message);
        verifyNoMoreInteractions(auditTrail);
    }

    public void testRealmAuthenticateThrowingException() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        doThrow(authenticationError("realm doesn't like authenticate"))
            .when(secondRealm).authenticate(eq(token), any(ActionListener.class));
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
        try {
            authenticateBlocking("_action", message, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like authenticate"));
            verify(auditTrail).authenticationFailed(reqId, token, "_action", message);
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
            String reqId = expectAuditRequestId();
            verify(auditTrail).authenticationFailed(reqId, token, restRequest);
        }
    }

    public void testRealmLookupThrowingException() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, new User("lookup user", new String[]{"user"}));
        mockRealmLookupReturnsNull(firstRealm, "run_as");
        doThrow(authenticationError("realm doesn't want to lookup"))
            .when(secondRealm).lookupUser(eq("run_as"), any(ActionListener.class));
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);

        try {
            authenticateBlocking("_action", message, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't want to lookup"));
            verify(auditTrail).authenticationFailed(reqId, token, "_action", message);
        }
    }

    public void testRealmLookupThrowingExceptionRest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, new User("lookup user", new String[]{"user"}));
        mockRealmLookupReturnsNull(firstRealm, "run_as");
        doThrow(authenticationError("realm doesn't want to lookup"))
                .when(secondRealm).lookupUser(eq("run_as"), any(ActionListener.class));
        try {
            authenticateBlocking(restRequest);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't want to lookup"));
            String reqId = expectAuditRequestId();
            verify(auditTrail).authenticationFailed(reqId, token, restRequest);
        }
    }

    public void testRunAsLookupSameRealm() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        final User user = new User("lookup user", new String[]{"user"}, "lookup user", "lookup@foo.foo",
                Collections.singletonMap("foo", "bar"), true);
        mockAuthenticate(secondRealm, token, user);
        mockRealmLookupReturnsNull(firstRealm, "run_as");
        doAnswer((i) -> {
            ActionListener<User> listener = (ActionListener<User>) i.getArguments()[1];
            listener.onResponse(new User("looked up user", new String[]{"some role"}));
            return null;
        }).when(secondRealm).lookupUser(eq("run_as"), any(ActionListener.class));

        final AtomicBoolean completed = new AtomicBoolean(false);
        ActionListener<Authentication> listener = ActionListener.wrap(result -> {
            assertThat(result, notNullValue());
            User authenticated = result.getUser();

            assertThat(authenticated.principal(), is("looked up user"));
            assertThat(authenticated.roles(), arrayContaining("some role"));
            assertThreadContextContainsAuthentication(result);

            assertThat(SystemUser.is(authenticated), is(false));
            assertThat(authenticated.isRunAs(), is(true));
            User authUser = authenticated.authenticatedUser();
            assertThat(authUser.principal(), is("lookup user"));
            assertThat(authUser.roles(), arrayContaining("user"));
            assertEquals(user.metadata(), authUser.metadata());
            assertEquals(user.email(), authUser.email());
            assertEquals(user.enabled(), authUser.enabled());
            assertEquals(user.fullName(), authUser.fullName());


            setCompletedToTrue(completed);
        }, this::logAndFail);

        // we do not actually go async
        if (randomBoolean()) {
            service.authenticate("_action", message, (User)null, listener);
        } else {
            service.authenticate(restRequest, listener);
        }
        assertTrue(completed.get());
    }

    @SuppressWarnings("unchecked")
    public void testRunAsLookupDifferentRealm() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, new User("lookup user", new String[]{"user"}));
        doAnswer((i) -> {
            ActionListener<User> listener = (ActionListener<User>) i.getArguments()[1];
            listener.onResponse(new User("looked up user", new String[]{"some role"}));
            return null;
        }).when(firstRealm).lookupUser(eq("run_as"), any(ActionListener.class));

        final AtomicBoolean completed = new AtomicBoolean(false);
        ActionListener<Authentication> listener = ActionListener.wrap(result -> {
            assertThat(result, notNullValue());
            User authenticated = result.getUser();

            assertThat(SystemUser.is(authenticated), is(false));
            assertThat(authenticated.isRunAs(), is(true));
            assertThat(authenticated.authenticatedUser().principal(), is("lookup user"));
            assertThat(authenticated.authenticatedUser().roles(), arrayContaining("user"));
            assertThat(authenticated.principal(), is("looked up user"));
            assertThat(authenticated.roles(), arrayContaining("some role"));
            assertThreadContextContainsAuthentication(result);
            setCompletedToTrue(completed);
        }, this::logAndFail);

        // call service asynchronously but it doesn't actually go async
        if (randomBoolean()) {
            service.authenticate("_action", message, (User)null, listener);
        } else {
            service.authenticate(restRequest, listener);
        }
        assertTrue(completed.get());
    }

    public void testRunAsWithEmptyRunAsUsernameRest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        User user = new User("lookup user", new String[]{"user"});
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, user);

        try {
            authenticateBlocking(restRequest);
            fail("exception should be thrown");
        } catch (ElasticsearchException e) {
            String reqId = expectAuditRequestId();
            verify(auditTrail).runAsDenied(eq(reqId), any(Authentication.class), eq(restRequest), eq(Role.EMPTY.names()));
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testRunAsWithEmptyRunAsUsername() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        User user = new User("lookup user", new String[]{"user"});
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "");
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, user);

        try {
            authenticateBlocking("_action", message, null);
            fail("exception should be thrown");
        } catch (ElasticsearchException e) {
            verify(auditTrail).runAsDenied(eq(reqId), any(Authentication.class), eq("_action"), eq(message), eq(Role.EMPTY.names()));
            verifyNoMoreInteractions(auditTrail);
        }
    }

    @SuppressWarnings("unchecked")
    public void testAuthenticateTransportDisabledRunAsUser() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "run_as");
        final String reqId = AuditUtil.getOrGenerateRequestId(threadContext);
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, new User("lookup user", new String[]{"user"}));
        mockRealmLookupReturnsNull(firstRealm, "run_as");
        doAnswer((i) -> {
            ActionListener<User> listener = (ActionListener<User>) i.getArguments()[1];
            listener.onResponse(new User("looked up user", new String[]{"some role"}, null, null, null, false));
            return null;
        }).when(secondRealm).lookupUser(eq("run_as"), any(ActionListener.class));
        User fallback = randomBoolean() ? SystemUser.INSTANCE : null;
        ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking("_action", message, fallback));
        verify(auditTrail).authenticationFailed(reqId, token, "_action", message);
        verifyNoMoreInteractions(auditTrail);
        assertAuthenticationException(e);
    }

    public void testAuthenticateRestDisabledRunAsUser() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(threadContext)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        mockAuthenticate(secondRealm, token, new User("lookup user", new String[]{"user"}));
        mockRealmLookupReturnsNull(firstRealm, "run_as");
        doAnswer((i) -> {
            @SuppressWarnings("unchecked")
            ActionListener<User> listener = (ActionListener<User>) i.getArguments()[1];
            listener.onResponse(new User("looked up user", new String[]{"some role"}, null, null, null, false));
            return null;
        }).when(secondRealm).lookupUser(eq("run_as"), any(ActionListener.class));

        ElasticsearchSecurityException e =
                expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking(restRequest));
        String reqId = expectAuditRequestId();
        verify(auditTrail).authenticationFailed(reqId, token, restRequest);
        verifyNoMoreInteractions(auditTrail);
        assertAuthenticationException(e);
    }

    public void testAuthenticateWithToken() throws Exception {
        User user = new User("_username", "r1");
        final AtomicBoolean completed = new AtomicBoolean(false);
        final Authentication expected = new Authentication(user, new RealmRef("realm", "custom", "node"), null);
        PlainActionFuture<Tuple<UserToken, String>> tokenFuture = new PlainActionFuture<>();
        try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
            Authentication originatingAuth = new Authentication(new User("creator"), new RealmRef("test", "test", "test"), null);
            tokenService.createUserToken(expected, originatingAuth, tokenFuture, Collections.emptyMap(), true);
        }
        String token = tokenService.getUserTokenString(tokenFuture.get().v1());
        when(client.prepareMultiGet()).thenReturn(new MultiGetRequestBuilder(client, MultiGetAction.INSTANCE));
        mockGetTokenFromId(tokenFuture.get().v1(), false, client);
        when(securityIndex.isAvailable()).thenReturn(true);
        when(securityIndex.indexExists()).thenReturn(true);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("Authorization", "Bearer " + token);
            service.authenticate("_action", message, (User)null, ActionListener.wrap(result -> {
                assertThat(result, notNullValue());
                assertThat(result.getUser(), is(user));
                assertThat(result.getLookedUpBy(), is(nullValue()));
                assertThat(result.getAuthenticatedBy(), is(notNullValue()));
                assertEquals(expected, result);
                setCompletedToTrue(completed);
            }, this::logAndFail));
        }
        assertTrue(completed.get());
        verify(auditTrail).authenticationSuccess(anyString(), eq("realm"), eq(user), eq("_action"), same(message));
        verifyNoMoreInteractions(auditTrail);
    }

    public void testInvalidToken() throws Exception {
        final User user = new User("_username", "r1");
        when(firstRealm.token(threadContext)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        mockAuthenticate(firstRealm, token, user);
        final int numBytes = randomIntBetween(TokenService.MINIMUM_BYTES, TokenService.MINIMUM_BYTES + 32);
        final byte[] randomBytes = new byte[numBytes];
        random().nextBytes(randomBytes);
        final CountDownLatch latch = new CountDownLatch(1);
        final Authentication expected = new Authentication(user, new RealmRef(firstRealm.name(), firstRealm.type(), "authc_test"), null);
        AtomicBoolean success = new AtomicBoolean(false);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("Authorization", "Bearer " + Base64.getEncoder().encodeToString(randomBytes));
            service.authenticate("_action", message, (User)null, ActionListener.wrap(result -> {
                assertThat(result, notNullValue());
                assertThat(result.getUser(), is(user));
                assertThat(result.getLookedUpBy(), is(nullValue()));
                assertThat(result.getAuthenticatedBy(), is(notNullValue()));
                assertThreadContextContainsAuthentication(result);
                assertEquals(expected, result);
                success.set(true);
                latch.countDown();
            }, e -> {
                if (e instanceof IllegalStateException) {
                    assertThat(e.getMessage(), containsString("array length must be <= to " + ArrayUtil.MAX_ARRAY_LENGTH  + " but was: "));
                    latch.countDown();
                } else if (e instanceof NegativeArraySizeException) {
                    assertThat(e.getMessage(), containsString("array size must be positive but was: "));
                    latch.countDown();
                } else {
                    logger.error("unexpected exception", e);
                    latch.countDown();
                    fail("unexpected exception: " + e.getMessage());
                }
            }));
        } catch (IllegalStateException ex) {
            assertThat(ex.getMessage(), containsString("array length must be <= to " + ArrayUtil.MAX_ARRAY_LENGTH  + " but was: "));
            latch.countDown();
        } catch (NegativeArraySizeException ex) {
            assertThat(ex.getMessage(), containsString("array size must be positive but was: "));
            latch.countDown();
        }

        // we need to use a latch here because the key computation goes async on another thread!
        latch.await();
        if (success.get()) {
            final String realmName = firstRealm.name();
            verify(auditTrail).authenticationSuccess(anyString(), eq(realmName), eq(user), eq("_action"), same(message));
        }
        verifyNoMoreInteractions(auditTrail);
    }

    public void testExpiredToken() throws Exception {
        when(securityIndex.isAvailable()).thenReturn(true);
        when(securityIndex.indexExists()).thenReturn(true);
        User user = new User("_username", "r1");
        final Authentication expected = new Authentication(user, new RealmRef("realm", "custom", "node"), null);
        PlainActionFuture<Tuple<UserToken, String>> tokenFuture = new PlainActionFuture<>();
        try (ThreadContext.StoredContext ctx = threadContext.stashContext()) {
            Authentication originatingAuth = new Authentication(new User("creator"), new RealmRef("test", "test", "test"), null);
            tokenService.createUserToken(expected, originatingAuth, tokenFuture, Collections.emptyMap(), true);
        }
        String token = tokenService.getUserTokenString(tokenFuture.get().v1());
        mockGetTokenFromId(tokenFuture.get().v1(), true, client);
        doAnswer(invocationOnMock -> {
            ((Runnable) invocationOnMock.getArguments()[1]).run();
            return null;
        }).when(securityIndex).prepareIndexIfNeededThenExecute(any(Consumer.class), any(Runnable.class));

        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("Authorization", "Bearer " + token);
            ElasticsearchSecurityException e =
                    expectThrows(ElasticsearchSecurityException.class, () -> authenticateBlocking("_action", message, null));
            assertEquals(RestStatus.UNAUTHORIZED, e.status());
            assertEquals("token expired", e.getMessage());
        }
    }

    private static class InternalMessage extends TransportMessage {
    }

    void assertThreadContextContainsAuthentication(Authentication authentication) throws IOException {
        Authentication contextAuth = threadContext.getTransient(AuthenticationField.AUTHENTICATION_KEY);
        assertThat(contextAuth, notNullValue());
        assertThat(contextAuth, is(authentication));
        assertThat(threadContext.getHeader(AuthenticationField.AUTHENTICATION_KEY), equalTo((Object) authentication.encode()));
    }

    @SuppressWarnings("unchecked")
    private void mockAuthenticate(Realm realm, AuthenticationToken token, User user) {
        final boolean separateThread = randomBoolean();
        doAnswer(i -> {
            ActionListener<AuthenticationResult> listener = (ActionListener<AuthenticationResult>) i.getArguments()[1];
            Runnable run = () -> {
                if (user == null) {
                    listener.onResponse(AuthenticationResult.notHandled());
                } else {
                    listener.onResponse(AuthenticationResult.success(user));
                }
            };
            if (separateThread) {
                final Thread thread = new Thread(run);
                thread.start();
                thread.join();
            } else {
                run.run();
            }
            return null;
        }).when(realm).authenticate(eq(token), any(ActionListener.class));
    }

    @SuppressWarnings("unchecked")
    private void mockAuthenticate(Realm realm, AuthenticationToken token, Exception e, boolean terminate) {
        doAnswer((i) -> {
            ActionListener<AuthenticationResult> listener = (ActionListener<AuthenticationResult>) i.getArguments()[1];
            if (terminate) {
                listener.onResponse(AuthenticationResult.terminate("terminate authc process", e));
            } else {
                listener.onResponse(AuthenticationResult.unsuccessful("unsuccessful, but continue authc process", e));
            }
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

    private String expectAuditRequestId() {
        String reqId = AuditUtil.extractRequestId(threadContext);
        assertThat(reqId, not(isEmptyOrNullString()));
        return reqId;
    }

    @SuppressWarnings("unchecked")
    private static void mockRealmLookupReturnsNull(Realm realm, String username) {
        doAnswer((i) -> {
            ActionListener<?> listener = (ActionListener<?>) i.getArguments()[1];
            listener.onResponse(null);
            return null;
        }).when(realm).lookupUser(eq(username), any(ActionListener.class));
    }

    static class TestRealms extends Realms {

        TestRealms(Settings settings, Environment env, Map<String, Factory> factories, XPackLicenseState licenseState,
                   ThreadContext threadContext, ReservedRealm reservedRealm, List<Realm> realms, List<Realm> internalRealms)
                throws Exception {
            super(settings, env, factories, licenseState, threadContext, reservedRealm);
            this.realms = realms;
            this.standardRealmsOnly = internalRealms;
        }
    }

    private void logAndFail(Exception e) {
        logger.error("unexpected exception", e);
        fail("unexpected exception " + e.getMessage());
    }

    private void setCompletedToTrue(AtomicBoolean completed) {
        assertTrue(completed.compareAndSet(false, true));
    }
}
