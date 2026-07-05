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

import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.sameInstance;

public class CloudCredentialManagerTests extends ESTestCase {

    private CloudCredentialManager manager;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        manager = new CloudCredentialManager.Noop();
    }

    public void testNoopReportsNoCredential() {
        var threadContext = new ThreadContext(Settings.EMPTY);
        assertFalse(manager.hasCloudManagedCredential(threadContext));
    }

    public void testNoopExtractThrows() {
        var threadContext = new ThreadContext(Settings.EMPTY);
        expectThrows(UnsupportedOperationException.class, () -> manager.extractCloudManagedCredential(threadContext));
    }

    public void testNoopInjectThrows() {
        var threadContext = new ThreadContext(Settings.EMPTY);
        var credential = new CloudCredential(new SecureString("v".toCharArray()));
        expectThrows(UnsupportedOperationException.class, () -> manager.injectCloudManagedCredential(threadContext, credential));
    }

    public void testNoopResolverOfPersistedThrows() {
        var persisted = new PersistedCloudCredential("an-id", new SecureString("v".toCharArray()));
        expectThrows(UnsupportedOperationException.class, () -> manager.resolverOf(persisted));
    }

    public void testResolverOfCredentialResolvesToWrappedInstance() {
        var credential = new CloudCredential(new SecureString("v".toCharArray()));
        var resolver = manager.resolverOf(credential);
        assertThat(resolver.resolve(), sameInstance(credential));
    }

    public void testResolverOfCredentialRejectsNull() {
        expectThrows(NullPointerException.class, () -> manager.resolverOf((CloudCredential) null));
    }

    public void testNoopWrapClientReturnsDelegateForAllOverloads() {
        try (var threadPool = new TestThreadPool(getTestName())) {
            var delegate = TestAssertingClient.assertingNoDispatch(threadPool);

            // Null inputs short-circuit to delegate on every overload.
            assertThat(manager.wrapClient(delegate, (CloudCredential) null), sameInstance(delegate));
            assertThat(manager.wrapClient(delegate, (PersistedCloudCredential) null), sameInstance(delegate));

            // Non-null inputs must also be silent no-ops on Noop so a feature-flag flip-down does
            // not crash a request path that carries a dormant persisted credential from a
            // previously-active config.
            var credential = new CloudCredential(new SecureString("v".toCharArray()));
            var persisted = new PersistedCloudCredential("an-id", new SecureString("v".toCharArray()));
            CloudCredentialResolver throwingResolver = () -> { throw new AssertionError("resolver must not be invoked on Noop"); };

            assertThat(manager.wrapClient(delegate, throwingResolver), sameInstance(delegate));
            assertThat(manager.wrapClient(delegate, credential), sameInstance(delegate));
            assertThat(manager.wrapClient(delegate, persisted), sameInstance(delegate));
        }
    }

    /**
     * {@link NoOpClient} that fails any inadvertent dispatch — used to assert that a
     * {@link CloudCredentialManager.Noop} {@code wrapClient} call does not eagerly invoke
     * {@code doExecute}.
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
