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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.instanceOf;
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
        expectThrows(IllegalStateException.class, () -> manager.extractCloudManagedCredential(threadContext));
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

    public void testWrapClientWithNullResolveSkipsInjectAndDispatches() {
        try (var threadPool = new TestThreadPool(getTestName())) {
            CloudCredentialResolver nullResolver = () -> null;
            var request = new NoopRequest();
            var dispatched = new AtomicBoolean();
            var delegate = new TestAssertingClient(threadPool, (req, ctx) -> {
                assertThat(req, sameInstance(request));
                dispatched.set(true);
            });

            var wrapped = manager.wrapClient(delegate, nullResolver);
            wrapped.execute(NoopAction.INSTANCE, request, ActionListener.wrap(r -> {}, e -> {}));

            assertTrue("delegate#doExecute should have fired", dispatched.get());
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
