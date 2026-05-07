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

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
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

    public void testSourceOfCredentialResolvesToWrappedInstance() {
        var credential = new CloudCredential(new SecureString("v".toCharArray()));
        var source = manager.sourceOf(credential);
        assertThat(source.resolve(), sameInstance(credential));
    }

    public void testSourceOfCredentialRejectsNull() {
        expectThrows(NullPointerException.class, () -> manager.sourceOf((CloudCredential) null));
    }

    public void testSourceOfPersistedThrowsOnDefault() {
        var persisted = new PersistedCloudCredential("an-id", new CloudCredential(new SecureString("v".toCharArray())));
        expectThrows(UnsupportedOperationException.class, () -> manager.sourceOf(persisted));
    }

    public void testWrapClientWithNullSourceReturnsDelegateUnchanged() {
        try (var threadPool = new TestThreadPool(getTestName())) {
            var delegate = new TrackingClient(threadPool);
            assertThat(manager.wrapClient(delegate, (CloudCredentialSource) null), sameInstance(delegate));
            assertThat(manager.wrapClient(delegate, (CloudCredential) null), sameInstance(delegate));
            assertThat(manager.wrapClient(delegate, (PersistedCloudCredential) null), sameInstance(delegate));
        }
    }

    public void testWrapClientRoutesResolveFailureToListener() {
        try (var threadPool = new TestThreadPool(getTestName())) {
            var delegate = new TrackingClient(threadPool);
            var failure = new RuntimeException("boom");
            CloudCredentialSource throwingSource = () -> { throw failure; };

            var wrapped = manager.wrapClient(delegate, throwingSource);
            var capturedFailure = new AtomicReference<Exception>();
            var capturedSuccess = new AtomicReference<Object>();
            wrapped.execute(NoopAction.INSTANCE, new NoopRequest(), ActionListener.wrap(capturedSuccess::set, capturedFailure::set));

            assertNull(capturedSuccess.get());
            assertThat(capturedFailure.get(), sameInstance(failure));
            assertThat(delegate.lastRequest, is(nullValue()));
        }
    }

    public void testWrapClientRoutesInjectFailureToListener() {
        try (var threadPool = new TestThreadPool(getTestName())) {
            var delegate = new TrackingClient(threadPool);
            var credential = new CloudCredential(new SecureString("v".toCharArray()));

            var wrapped = manager.wrapClient(delegate, credential);
            var capturedFailure = new AtomicReference<Exception>();
            wrapped.execute(NoopAction.INSTANCE, new NoopRequest(), ActionListener.wrap(r -> {}, capturedFailure::set));

            assertThat(capturedFailure.get(), notNullValue());
            assertThat(capturedFailure.get(), instanceOf(UnsupportedOperationException.class));
            assertThat(delegate.lastRequest, is(nullValue()));
        }
    }

    public void testWrapClientWithNullResolveSkipsInjectAndDispatches() {
        try (var threadPool = new TestThreadPool(getTestName())) {
            var delegate = new TrackingClient(threadPool);
            CloudCredentialSource nullSource = () -> null;

            // The Default manager would throw on inject if called; absent injection (null resolve)
            // means the wrapped client just delegates straight through.
            var wrapped = manager.wrapClient(delegate, nullSource);
            var request = new NoopRequest();
            wrapped.execute(NoopAction.INSTANCE, request, ActionListener.wrap(r -> {}, e -> {}));

            assertThat(delegate.lastRequest, sameInstance(request));
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

    /** {@link NoOpClient} that records the last request received via {@code execute(...)}. */
    private static class TrackingClient extends NoOpClient {
        volatile ActionRequest lastRequest;

        TrackingClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            lastRequest = request;
            super.doExecute(action, request, listener);
        }
    }
}
