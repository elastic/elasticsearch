/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.CustomAuthenticator;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class PluggableAuthenticatorChainTests extends ESTestCase {

    private ThreadContext threadContext;

    private static class TestTokenA implements AuthenticationToken {
        private final String value;

        TestTokenA(String value) {
            this.value = value;
        }

        @Override
        public String principal() {
            return "user-" + value;
        }

        @Override
        public Object credentials() {
            return value;
        }

        @Override
        public void clearCredentials() {
            // no-op
        }

        public String getValue() {
            return value;
        }
    }

    private static class TestTokenB implements AuthenticationToken {
        private final String value;

        TestTokenB(String value) {
            this.value = value;
        }

        @Override
        public String principal() {
            return "user-" + value;
        }

        @Override
        public Object credentials() {
            return value;
        }

        @Override
        public void clearCredentials() {
            // no-op
        }

        public String getValue() {
            return value;
        }
    }

    public class TokenAAuthenticator implements CustomAuthenticator {

        private final String id;
        private boolean succeed;

        public TokenAAuthenticator() {
            this("1", true);
        }

        public TokenAAuthenticator(String id, boolean succeed) {
            this.id = id;
            this.succeed = succeed;
        }

        @Override
        public boolean supports(AuthenticationToken token) {
            return token instanceof TestTokenA;
        }

        @Override
        public @Nullable AuthenticationToken extractToken(ThreadContext context) {
            return new TestTokenA("foo");
        }

        @Override
        public void authenticate(@Nullable AuthenticationToken token, ActionListener<AuthenticationResult<Authentication>> listener) {
            if (token instanceof TestTokenA testToken) {
                if (succeed) {
                    User user = new User("token-a-auth-user-" + id + "-" + testToken.getValue());
                    Authentication auth = AuthenticationTestHelper.builder().user(user).build(false);
                    listener.onResponse(AuthenticationResult.success(auth));
                } else {
                    listener.onResponse(AuthenticationResult.terminate("token-a-fail-" + id + "-" + testToken.getValue()));
                }
            } else {
                listener.onResponse(AuthenticationResult.notHandled());
            }
        }
    }

    public class TokenBAuthenticator implements CustomAuthenticator {

        private final String id;

        public TokenBAuthenticator() {
            id = "1";
        }

        public TokenBAuthenticator(String id) {
            this.id = id;
        }

        @Override
        public boolean supports(AuthenticationToken token) {
            return token instanceof TestTokenB;
        }

        @Override
        public @Nullable AuthenticationToken extractToken(ThreadContext context) {
            return new TestTokenB("foo");
        }

        @Override
        public void authenticate(@Nullable AuthenticationToken token, ActionListener<AuthenticationResult<Authentication>> listener) {
            if (token instanceof TestTokenB testToken) {
                User user = new User("token-b-auth-user-" + id + "-" + testToken.getValue());
                Authentication auth = AuthenticationTestHelper.builder().user(user).build(false);
                listener.onResponse(AuthenticationResult.success(auth));
            } else {
                listener.onResponse(AuthenticationResult.notHandled());
            }
        }
    }

    @Before
    public void init() {
        final Settings settings = Settings.builder().build();
        threadContext = new ThreadContext(settings);
    }

    public void testAuthenticateWithTokenAPickedUpByTokenAAuthenticatorInCustomChain() throws Exception {

        PluggableAuthenticatorChain chain = new PluggableAuthenticatorChain(List.of(new TokenAAuthenticator(), new TokenBAuthenticator()));
        TestTokenA testToken = new TestTokenA("test-value");

        Authenticator.Context context = createContext();
        context.addAuthenticationToken(testToken);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<AuthenticationResult<Authentication>> resultRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        ActionListener<AuthenticationResult<Authentication>> listener = new ActionListener<>() {
            @Override
            public void onResponse(AuthenticationResult<Authentication> result) {
                resultRef.set(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        };

        chain.authenticate(context, listener);
        latch.await();

        if (exceptionRef.get() != null) {
            throw new AssertionError("Authentication failed with exception", exceptionRef.get());
        }

        AuthenticationResult<Authentication> result = resultRef.get();
        assertThat(result, notNullValue());
        assertThat(result.isAuthenticated(), equalTo(true));

        Authentication auth = result.getValue();
        assertThat(auth.getEffectiveSubject().getUser().principal(), equalTo("token-a-auth-user-1-test-value"));
    }

    public void testAuthenticateWithTokenAPickedUpByTokenAAuthenticatorInCustomChainWithChainOrderFlipped() throws Exception {

        PluggableAuthenticatorChain chain = new PluggableAuthenticatorChain(List.of(new TokenBAuthenticator(), new TokenAAuthenticator()));
        TestTokenA testToken = new TestTokenA("test-value");

        Authenticator.Context context = createContext();
        context.addAuthenticationToken(testToken);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<AuthenticationResult<Authentication>> resultRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        ActionListener<AuthenticationResult<Authentication>> listener = new ActionListener<>() {
            @Override
            public void onResponse(AuthenticationResult<Authentication> result) {
                resultRef.set(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        };

        chain.authenticate(context, listener);
        latch.await();

        if (exceptionRef.get() != null) {
            throw new AssertionError("Authentication failed with exception", exceptionRef.get());
        }

        AuthenticationResult<Authentication> result = resultRef.get();
        assertThat(result, notNullValue());
        assertThat(result.isAuthenticated(), equalTo(true));

        Authentication auth = result.getValue();
        assertThat(auth.getEffectiveSubject().getUser().principal(), equalTo("token-a-auth-user-1-test-value"));
    }

    public void testAuthenticateWhenTokenSupportedByBothAuthenticatorsInChain() throws Exception {

        PluggableAuthenticatorChain chain = new PluggableAuthenticatorChain(
            List.of(new TokenAAuthenticator("foo", true), new TokenAAuthenticator("bar", true))
        );
        TestTokenA testToken = new TestTokenA("test-value");

        Authenticator.Context context = createContext();
        context.addAuthenticationToken(testToken);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<AuthenticationResult<Authentication>> resultRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        ActionListener<AuthenticationResult<Authentication>> listener = new ActionListener<>() {
            @Override
            public void onResponse(AuthenticationResult<Authentication> result) {
                resultRef.set(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        };

        chain.authenticate(context, listener);
        latch.await();

        if (exceptionRef.get() != null) {
            throw new AssertionError("Authentication failed with exception", exceptionRef.get());
        }

        AuthenticationResult<Authentication> result = resultRef.get();
        assertThat(result, notNullValue());
        assertThat(result.isAuthenticated(), equalTo(true));

        Authentication auth = result.getValue();
        assertThat(auth.getEffectiveSubject().getUser().principal(), equalTo("token-a-auth-user-foo-test-value")); // id of first
    }

    public void testAuthenticateWhenTokenSupportedByNoAuthenticatorsInChain() throws Exception {

        PluggableAuthenticatorChain chain = new PluggableAuthenticatorChain(
            List.of(new TokenAAuthenticator("foo", true), new TokenAAuthenticator("bar", true))
        );
        AuthenticationToken unknownToken = new AuthenticationToken() {
            @Override
            public String principal() {
                return "unknown";
            }

            @Override
            public Object credentials() {
                return null;
            }

            @Override
            public void clearCredentials() {
                // no-op
            }
        };

        Authenticator.Context context = createContext();
        context.addAuthenticationToken(unknownToken);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<AuthenticationResult<Authentication>> resultRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        ActionListener<AuthenticationResult<Authentication>> listener = new ActionListener<>() {
            @Override
            public void onResponse(AuthenticationResult<Authentication> result) {
                resultRef.set(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        };

        chain.authenticate(context, listener);
        latch.await();

        if (exceptionRef.get() != null) {
            throw new AssertionError("Authentication failed with exception", exceptionRef.get());
        }

        AuthenticationResult<Authentication> result = resultRef.get();
        assertThat(result, notNullValue());
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.CONTINUE));
    }

    public void testAuthenticationTermination() throws Exception {
        final PluggableAuthenticatorChain chain = new PluggableAuthenticatorChain(List.of(new TokenAAuthenticator("terminate", false)));
        final TestTokenA token = new TestTokenA("err");
        final Authenticator.Context context = createContext();
        context.addAuthenticationToken(token);

        Loggers.setLevel(LogManager.getLogger(PluggableAuthenticatorChain.class), Level.DEBUG);
        try (MockLog mockLog = MockLog.capture(PluggableAuthenticatorChain.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "debug-auth-failure",
                    PluggableAuthenticatorChain.class.getName(),
                    Level.DEBUG,
                    "Authentication of token [user-err] was terminated: token-a-fail-terminate-err (caused by: null)"
                )
            );

            final PlainActionFuture<AuthenticationResult<Authentication>> future = new PlainActionFuture<>();
            chain.authenticate(context, future);
            mockLog.assertAllExpectationsMatched();

            final ElasticsearchSecurityException ex = expectThrows(ElasticsearchSecurityException.class, () -> future.actionGet());
            assertThat(ex, throwableWithMessage("mock-request-failure"));
            assertThat(ex.status(), equalTo(RestStatus.UNAUTHORIZED));
        }
    }

    private Authenticator.Context createContext() {
        final var request = Mockito.mock(AuthenticationService.AuditableRequest.class);
        Mockito.when(request.authenticationFailed(Mockito.any(AuthenticationToken.class)))
            .thenAnswer(inv -> new ElasticsearchSecurityException("mock-request-failure", RestStatus.UNAUTHORIZED));
        return new Authenticator.Context(threadContext, request, null, true, null);
    }
}
