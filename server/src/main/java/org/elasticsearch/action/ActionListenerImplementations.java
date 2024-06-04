/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.lang.ref.Cleaner;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Internal implementation details of the various utility methods on {@link ActionListener}.
 */
class ActionListenerImplementations {

    private static final Cleaner cleaner = Cleaner.create();

    private ActionListenerImplementations() {
        // no instances
    }

    @SuppressWarnings("rawtypes")
    static final ActionListener NOOP = new ActionListener() {
        @Override
        public void onResponse(Object o) {}

        @Override
        public void onFailure(Exception e) {}

        @Override
        public String toString() {
            return "NoopActionListener";
        }
    };

    static Runnable runnableFromReleasable(Releasable releasable) {
        return new Runnable() {
            @Override
            public void run() {
                Releasables.closeExpectNoException(releasable);
            }

            @Override
            public String toString() {
                return "release[" + releasable + "]";
            }
        };
    }

    static void safeAcceptException(Consumer<Exception> consumer, Exception e) {
        assert e != null;
        try {
            consumer.accept(e);
        } catch (RuntimeException ex) {
            // noinspection ConstantConditions
            if (e != null && ex != e) {
                ex.addSuppressed(e);
            }
            expectNoException(ex);
        }
    }

    static void safeOnFailure(ActionListener<?> listener, Exception e) {
        safeAcceptException(listener::onFailure, e);
    }

    static final class MappedActionListener<Response, MappedResponse> extends DelegatingActionListener<Response, MappedResponse> {

        private final CheckedFunction<Response, MappedResponse, Exception> fn;

        MappedActionListener(CheckedFunction<Response, MappedResponse, Exception> fn, ActionListener<MappedResponse> delegate) {
            super(delegate);
            this.fn = fn;
        }

        @Override
        public void onResponse(Response response) {
            MappedResponse mapped;
            try {
                mapped = fn.apply(response);
            } catch (Exception e) {
                onFailure(e);
                return;
            }
            try {
                delegate.onResponse(mapped);
            } catch (RuntimeException e) {
                expectNoException(e);
            }
        }

        @Override
        public String toString() {
            return super.toString() + "/" + fn;
        }

        @Override
        public <T> ActionListener<T> map(CheckedFunction<T, Response, Exception> fn) {
            return new MappedActionListener<>(t -> this.fn.apply(fn.apply(t)), this.delegate);
        }

        @Override
        public <T> ActionListener<T> safeMap(Function<T, Response> fn) {
            return new MappedActionListener<>(t -> this.fn.apply(applyExpectNoExceptions(fn, t)), this.delegate);
        }
    }

    static final class SafeMappedActionListener<Response, MappedResponse> extends DelegatingActionListener<Response, MappedResponse> {

        private final Function<Response, MappedResponse> fn;

        SafeMappedActionListener(Function<Response, MappedResponse> fn, ActionListener<MappedResponse> delegate) {
            super(delegate);
            this.fn = fn;
        }

        @Override
        public void onResponse(Response response) {
            try {
                delegate.onResponse(applyExpectNoExceptions(fn, response));
            } catch (RuntimeException e) {
                expectNoException(e);
            }
        }

        @Override
        public String toString() {
            return super.toString() + "/" + fn;
        }

        @Override
        public <T> ActionListener<T> map(CheckedFunction<T, Response, Exception> fn) {
            return new MappedActionListener<>(t -> {
                var innerResult = fn.apply(t);
                return applyExpectNoExceptions(this.fn, innerResult);
            }, this.delegate);
        }

        @Override
        public <T> ActionListener<T> safeMap(Function<T, Response> fn) {
            return new SafeMappedActionListener<>(fn.andThen(this.fn), this.delegate);
        }
    }

    private static void expectNoException(RuntimeException e) {
        assert false : e;
        throw e;
    }

    private static <Response, MappedResponse> MappedResponse applyExpectNoExceptions(
        Function<Response, MappedResponse> fn,
        Response innerResult
    ) {
        try {
            return fn.apply(innerResult);
        } catch (RuntimeException e) {
            assert false : e;
            throw e;
        }
    }

    static final class DelegatingResponseActionListener<T> extends DelegatingActionListener<T, T> {

        private final BiConsumer<ActionListener<T>, Exception> bc;

        DelegatingResponseActionListener(ActionListener<T> delegate, BiConsumer<ActionListener<T>, Exception> bc) {
            super(delegate);
            this.bc = bc;
        }

        @Override
        public void onResponse(T t) {
            delegate.onResponse(t);
        }

        private void acceptException(Exception e) {
            bc.accept(delegate, e);
        }

        @Override
        public void onFailure(Exception e) {
            safeAcceptException(this::acceptException, e);
        }

        @Override
        public String toString() {
            return super.toString() + "/" + bc;
        }
    }

    /**
     * Replaces the onResponse handling of a given ActionListener with a lambda that receives both the original listener and a response.
     * This is useful when a listener is needed to do some additional work with a response before passing a response on to the original
     * listener.
     */
    static final class DelegatingFailureActionListener<T, R> extends DelegatingActionListener<T, R> {

        private final BiConsumer<ActionListener<R>, T> bc;

        DelegatingFailureActionListener(ActionListener<R> delegate, BiConsumer<ActionListener<R>, T> bc) {
            super(delegate);
            this.bc = bc;
        }

        @Override
        public void onResponse(T t) {
            try {
                bc.accept(delegate, t);
            } catch (RuntimeException e) {
                expectNoException(e);
            }
        }

        @Override
        public String toString() {
            return super.toString() + "/" + bc;
        }
    }

    /**
     * The same as {@link DelegatingFailureActionListener} with the addition of exception handling in {@link #onResponse(Object)} to forward
     * any exceptions to {@link #onFailure(Exception)}.
     */
    static final class ResponseWrappingActionListener<T, R> extends DelegatingActionListener<T, R> {

        private final CheckedBiConsumer<ActionListener<R>, T, ? extends Exception> bc;

        ResponseWrappingActionListener(ActionListener<R> delegate, CheckedBiConsumer<ActionListener<R>, T, ? extends Exception> bc) {
            super(delegate);
            this.bc = bc;
        }

        @Override
        public void onResponse(T t) {
            try {
                bc.accept(delegate, t);
            } catch (Exception e) {
                onFailure(e);
            }
        }

        @Override
        public String toString() {
            return super.toString() + "/" + bc;
        }
    }

    static final class RunAfterActionListener<T> extends DelegatingActionListener<T, T> {

        private final Runnable runAfter;

        RunAfterActionListener(ActionListener<T> delegate, Runnable runAfter) {
            super(delegate);
            this.runAfter = runAfter;
        }

        @Override
        public void onResponse(T response) {
            try {
                delegate.onResponse(response);
            } finally {
                runAfter.run();
            }
        }

        @Override
        public void onFailure(Exception e) {
            try {
                super.onFailure(e);
            } finally {
                runAfter.run();
            }
        }

        @Override
        public String toString() {
            return super.toString() + "/" + runAfter;
        }
    }

    static final class RunBeforeActionListener<T> extends DelegatingActionListener<T, T> {

        private final CheckedRunnable<?> runBefore;

        RunBeforeActionListener(ActionListener<T> delegate, CheckedRunnable<?> runBefore) {
            super(delegate);
            this.runBefore = runBefore;
        }

        @Override
        public void onResponse(T response) {
            try {
                runBefore.run();
            } catch (Exception ex) {
                super.onFailure(ex);
                return;
            }
            delegate.onResponse(response);
        }

        @Override
        public void onFailure(Exception e) {
            try {
                runBefore.run();
            } catch (Exception ex) {
                e.addSuppressed(ex);
            }
            super.onFailure(e);
        }

        @Override
        public String toString() {
            return super.toString() + "/" + runBefore;
        }
    }

    // Extend AtomicReference directly for minimum memory overhead and indirection.
    static final class NotifyOnceActionListener<Response> extends AtomicReference<ActionListener<Response>>
        implements
            ActionListener<Response> {

        NotifyOnceActionListener(ActionListener<Response> delegate) {
            super(delegate);
        }

        @Override
        public void onResponse(Response response) {
            final var acquired = getAndSet(null);
            if (acquired != null) {
                acquired.onResponse(response);
            }
        }

        @Override
        public void onFailure(Exception e) {
            final var acquired = getAndSet(null);
            if (acquired != null) {
                safeOnFailure(acquired, e);
            }
        }

        @Override
        public String toString() {
            return "notifyOnce[" + get() + "]";
        }
    }

    /**
     * An {@link ActionListener} that wraps a delegate, registering itself with a {@link Cleaner} that will check that
     * the delegate was called at least once prior to being garbage collected.
     * If the listener was not completed prior to becoming unreachable it will invoke the passed {@link NotCalledListener}
     * from the Cleaner thread.
     */
    static final class AssertAtLeastOnceActionListener<Response> extends DelegatingActionListener<Response, Response> {

        interface NotCalledListener {

            /**
             * Called when {@link CallTrackingActionListener#run()} is called prior to the listener being
             * called at least once.
             *
             * @param listener The listener that was not called at least once
             * @param createdAt The stack trace of where the listener was created
             */
            void onListenerNotCalled(ActionListener<?> listener, ElasticsearchException createdAt);
        }

        /**
         * This is the delegate that keeps track of whether {@link #onResponse(Object)} or {@link #onFailure(Exception)}
         * were called, and invokes the {@link NotCalledListener} if neither was called.
         * Because it is used as the "cleanup" action by the cleaner, it must be static, so it does not hold a reference
         * to the wrapper and prevent it becoming "phantom reachable".
         * See <a href="https://inside.java/2022/05/25/clean-cleaner/">the guidelines</a> for more details.
         */
        private static final class CallTrackingActionListener<R> implements Runnable, ActionListener<R> {

            private final ActionListener<R> delegate;
            private final ElasticsearchException created;
            private final NotCalledListener listener;
            private volatile boolean wasCalled = false;

            private CallTrackingActionListener(ActionListener<R> delegate, NotCalledListener listener) {
                this.delegate = delegate;
                this.created = new ElasticsearchException("Listener was never called");
                this.listener = listener;
            }

            @Override
            public void run() {
                if (wasCalled == false) {
                    listener.onListenerNotCalled(delegate, created);
                }
            }

            @Override
            public void onResponse(R r) {
                wasCalled = true;
                delegate.onResponse(r);
            }

            @Override
            public void onFailure(Exception e) {
                wasCalled = true;
                delegate.onFailure(e);
            }
        }

        /**
         * @param delegate The listener to wrap
         * @param listener The action to perform if we detect that it wasn't called (called on the Cleaner thread)
         */
        AssertAtLeastOnceActionListener(ActionListener<Response> delegate, NotCalledListener listener) {
            super(new CallTrackingActionListener<>(delegate, listener));
            cleaner.register(this, (Runnable) this.delegate);
        }

        @Override
        public void onResponse(Response response) {
            delegate.onResponse(response);
        }

        @Override
        public String toString() {
            return delegate.toString();
        }
    }
}
