/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.cloud;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class CloudCredentialManagerTests extends ESTestCase {

    private CloudCredentialManager manager;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        manager = new CloudCredentialManager.Default();
    }

    public void testHasReturnsFalseOnDefault() {
        var threadContext = new ThreadContext(Settings.EMPTY);
        assertFalse(manager.hasCloudManagedCredential(threadContext));
    }

    public void testExtractThrowsOnDefault() {
        var threadContext = new ThreadContext(Settings.EMPTY);
        expectThrows(UnsupportedOperationException.class, () -> manager.extractCloudManagedCredential(threadContext));
    }

    public void testInjectThrowsOnDefault() {
        var threadContext = new ThreadContext(Settings.EMPTY);
        var credential = new CloudCredential(new SecureString("v".toCharArray()));
        expectThrows(UnsupportedOperationException.class, () -> manager.injectCloudManagedCredential(threadContext, credential));
    }

    public void testResolverOfCredentialResolvesToWrappedInstance() {
        var credential = new CloudCredential(new SecureString("v".toCharArray()));
        var resolver = manager.resolverOf(credential);
        assertThat(resolver.resolve(), sameInstance(credential));
    }

    public void testResolverOfCredentialRejectsNull() {
        expectThrows(NullPointerException.class, () -> manager.resolverOf((CloudCredential) null));
    }

    public void testResolverOfPersistedThrowsOnDefault() {
        var persisted = new PersistedCloudCredential("an-id", new CloudCredential(new SecureString("v".toCharArray())));
        expectThrows(UnsupportedOperationException.class, () -> manager.resolverOf(persisted));
    }

    public void testWrapClientWithNullResolverReturnsDelegateUnchanged() {
        try (var threadPool = new TestThreadPool(getTestName())) {
            var delegate = TestAssertingClient.assertingNoDispatch(threadPool);
            assertThat(manager.wrapClient(delegate, (CloudCredentialResolver) null), sameInstance(delegate));
            assertThat(manager.wrapClient(delegate, (CloudCredential) null), sameInstance(delegate));
            assertThat(manager.wrapClient(delegate, (PersistedCloudCredential) null), sameInstance(delegate));
        }
    }

    public void testWrapClientRoutesResolveFailureToListener() {
        try (var threadPool = new TestThreadPool(getTestName())) {
            var delegate = TestAssertingClient.assertingNoDispatch(threadPool);
            var failure = new RuntimeException("boom");
            CloudCredentialResolver throwingResolver = () -> { throw failure; };

            var wrapped = manager.wrapClient(delegate, throwingResolver);
            var capturedFailure = new AtomicReference<Exception>();
            var capturedSuccess = new AtomicReference<Object>();
            wrapped.execute(NoopAction.INSTANCE, new NoopRequest(), ActionListener.wrap(capturedSuccess::set, capturedFailure::set));

            assertNull(capturedSuccess.get());
            assertThat(capturedFailure.get(), sameInstance(failure));
        }
    }

    public void testWrapClientRoutesInjectFailureToListener() {
        try (var threadPool = new TestThreadPool(getTestName())) {
            var delegate = TestAssertingClient.assertingNoDispatch(threadPool);
            var credential = new CloudCredential(new SecureString("v".toCharArray()));

            var wrapped = manager.wrapClient(delegate, credential);
            var capturedFailure = new AtomicReference<Exception>();
            wrapped.execute(NoopAction.INSTANCE, new NoopRequest(), ActionListener.wrap(r -> {}, capturedFailure::set));

            assertThat(capturedFailure.get(), notNullValue());
            assertThat(capturedFailure.get(), instanceOf(UnsupportedOperationException.class));
        }
    }

    public void testWrapClientResolvesInjectsAndDispatchesOnExecute() {
        try (var threadPool = new TestThreadPool(getTestName())) {
            final String testCredKey = "test_cred";
            var credential = new CloudCredential(new SecureString("v".toCharArray()));
            var request = new NoopRequest();
            var dispatched = new AtomicBoolean();

            var capturingManager = new CloudCredentialManager() {
                @Override
                public boolean hasCloudManagedCredential(ThreadContext threadContext) {
                    return false;
                }

                @Override
                public CloudCredential extractCloudManagedCredential(ThreadContext threadContext) {
                    throw new AssertionError("extract should not be called by wrapClient default");
                }

                @Override
                public void injectCloudManagedCredential(ThreadContext threadContext, CloudCredential cred) {
                    threadContext.putTransient(testCredKey, cred);
                }

                @Override
                public CloudCredentialResolver resolverOf(PersistedCloudCredential persisted) {
                    throw new AssertionError("resolverOf(PersistedCloudCredential) should not be called");
                }
            };

            var delegate = new TestAssertingClient(threadPool, (req, ctx) -> {
                assertThat(req, sameInstance(request));
                assertThat(ctx.getTransient(testCredKey), sameInstance(credential));
                dispatched.set(true);
            });

            var wrapped = capturingManager.wrapClient(delegate, credential);
            wrapped.execute(NoopAction.INSTANCE, request, ActionListener.wrap(r -> {}, e -> {}));

            assertTrue("delegate#doExecute should have fired", dispatched.get());
        }
    }

    public void testWrapClientRestoresCallerContextAfterExecute() {
        try (var threadPool = new TestThreadPool(getTestName())) {
            final String testCredKey = "test_cred";
            final String preExistingKey = "pre_existing";
            final String preExistingValue = "still_here";
            var credential = new CloudCredential(new SecureString("v".toCharArray()));
            var dispatchedSawCredential = new AtomicBoolean();

            var capturingManager = new CloudCredentialManager() {
                @Override
                public boolean hasCloudManagedCredential(ThreadContext threadContext) {
                    return threadContext.getTransient(testCredKey) != null;
                }

                @Override
                public CloudCredential extractCloudManagedCredential(ThreadContext threadContext) {
                    return threadContext.getTransient(testCredKey);
                }

                @Override
                public void injectCloudManagedCredential(ThreadContext threadContext, CloudCredential cred) {
                    threadContext.putTransient(testCredKey, cred);
                }

                @Override
                public CloudCredentialResolver resolverOf(PersistedCloudCredential persisted) {
                    throw new AssertionError("resolverOf(PersistedCloudCredential) should not be called");
                }
            };

            var delegate = new TestAssertingClient(threadPool, (req, ctx) -> {
                assertThat(ctx.getTransient(testCredKey), sameInstance(credential));
                assertThat(ctx.getTransient(preExistingKey), is(preExistingValue));
                dispatchedSawCredential.set(true);
            });

            var threadContext = threadPool.getThreadContext();
            threadContext.putTransient(preExistingKey, preExistingValue);
            assertNull("pre-condition: caller context must not contain the cloud token", threadContext.getTransient(testCredKey));

            var wrapped = capturingManager.wrapClient(delegate, credential);
            var listenerCalledUnderOriginalContext = new AtomicBoolean();
            wrapped.execute(NoopAction.INSTANCE, new NoopRequest(), ActionListener.wrap(r -> {
                assertNull(
                    "listener must fire under the original context, without the injected cloud token",
                    threadContext.getTransient(testCredKey)
                );
                assertThat(threadContext.getTransient(preExistingKey), is(preExistingValue));
                listenerCalledUnderOriginalContext.set(true);
            }, e -> fail("listener should succeed: " + e)));

            assertTrue("delegate#doExecute should have fired", dispatchedSawCredential.get());
            assertTrue("listener should have run", listenerCalledUnderOriginalContext.get());
            assertNull(
                "post-condition: caller context must not be polluted with the injected cloud token",
                threadContext.getTransient(testCredKey)
            );
            assertThat(
                "post-condition: pre-existing transients must survive",
                threadContext.getTransient(preExistingKey),
                is(preExistingValue)
            );
        }
    }

    public void testWrapClientComposesWithClientHelperStash() {
        try (var threadPool = new TestThreadPool(getTestName())) {
            final String testCredKey = "test_cred";
            final String authHeaderValue = "test-auth-value";
            var credential = new CloudCredential(new SecureString("v".toCharArray()));
            var dispatched = new AtomicBoolean();

            var capturingManager = new CloudCredentialManager() {
                @Override
                public boolean hasCloudManagedCredential(ThreadContext threadContext) {
                    return threadContext.getTransient(testCredKey) != null;
                }

                @Override
                public CloudCredential extractCloudManagedCredential(ThreadContext threadContext) {
                    return threadContext.getTransient(testCredKey);
                }

                @Override
                public void injectCloudManagedCredential(ThreadContext threadContext, CloudCredential cred) {
                    threadContext.putTransient(testCredKey, cred);
                }

                @Override
                public CloudCredentialResolver resolverOf(PersistedCloudCredential persisted) {
                    throw new AssertionError("resolverOf(PersistedCloudCredential) should not be called");
                }
            };

            var threadContext = threadPool.getThreadContext();
            threadContext.putTransient(testCredKey, credential);
            var captured = capturingManager.extractCloudManagedCredential(threadContext);

            var delegate = new TestAssertingClient(threadPool, (req, ctx) -> {
                assertThat(
                    "dispatched action should see the cloud token re-injected after ClientHelper stashed",
                    ctx.getTransient(testCredKey),
                    sameInstance(credential)
                );
                assertThat(
                    "dispatched action should see the security header copied by ClientHelper",
                    ctx.getHeader(AuthenticationField.AUTHENTICATION_KEY),
                    is(authHeaderValue)
                );
                dispatched.set(true);
            });

            var wrapped = capturingManager.wrapClient(delegate, captured);
            var listenerRan = new AtomicBoolean();

            ClientHelper.executeWithHeadersAsync(
                Map.of(AuthenticationField.AUTHENTICATION_KEY, authHeaderValue),
                "ml-test-origin",
                wrapped,
                NoopAction.INSTANCE,
                new NoopRequest(),
                ActionListener.wrap(r -> {
                    assertThat(
                        "listener should fire under the caller's original context (cloud token still present)",
                        threadContext.getTransient(testCredKey),
                        sameInstance(credential)
                    );
                    assertNull(
                        "listener should not see the stash-internal security header",
                        threadContext.getHeader(AuthenticationField.AUTHENTICATION_KEY)
                    );
                    listenerRan.set(true);
                }, e -> fail("listener should succeed: " + e))
            );

            assertTrue("delegate#doExecute should have fired", dispatched.get());
            assertTrue("listener should have run", listenerRan.get());
            assertThat(
                "post-condition: caller's original cloud token is still in scope",
                threadContext.getTransient(testCredKey),
                sameInstance(credential)
            );
            assertNull(
                "post-condition: ClientHelper's stash-internal header should not have leaked",
                threadContext.getHeader(AuthenticationField.AUTHENTICATION_KEY)
            );
        }
    }

    private static class NoopAction extends ActionType<ActionResponse.Empty> {
        static final NoopAction INSTANCE = new NoopAction();

        NoopAction() {
            super("test:noop");
        }
    }

    private static class NoopRequest extends ActionRequest {
        @Override
        public org.elasticsearch.action.ActionRequestValidationException validate() {
            return null;
        }
    }

    /**
     * {@link NoOpClient} that invokes a caller-supplied callback inside {@code doExecute}, so tests
     * can assert inline on the request and on the active {@link ThreadContext} at the moment of dispatch.
     */
    private static class TestAssertingClient extends NoOpClient {
        private final BiConsumer<ActionRequest, ThreadContext> onDispatch;

        static TestAssertingClient assertingNoDispatch(ThreadPool threadPool) {
            return new TestAssertingClient(threadPool, (req, ctx) -> fail("doExecute should not have been called"));
        }

        TestAssertingClient(ThreadPool threadPool, BiConsumer<ActionRequest, ThreadContext> onDispatch) {
            super(threadPool);
            this.onDispatch = onDispatch;
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            onDispatch.accept(request, threadPool().getThreadContext());
            super.doExecute(action, request, listener);
        }
    }
}
