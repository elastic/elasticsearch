/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.Assertions;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * A listener for action responses or failures.
 */
public interface ActionListener<Response> {
    /**
     * Handle action response. This response may constitute a failure or a
     * success but it is up to the listener to make that decision.
     */
    void onResponse(Response response);

    /**
     * A failure caused by an exception at some phase of the task.
     */
    void onFailure(Exception e);

    @SuppressWarnings("rawtypes")
    ActionListener NOOP = new ActionListener() {
        @Override
        public void onResponse(Object o) {

        }

        @Override
        public void onFailure(Exception e) {

        }

        @Override
        public String toString() {
            return "NoopActionListener";
        }
    };

    /**
     * @return a listener that does nothing
     */
    @SuppressWarnings("unchecked")
    static <T> ActionListener<T> noop() {
        return (ActionListener<T>) NOOP;
    }

    /**
     * Creates a listener that wraps this listener, mapping response values via the given mapping function and passing along
     * exceptions to this instance.
     *
     * Notice that it is considered a bug if the listener's onResponse or onFailure fails. onResponse failures will not call onFailure.
     *
     * If the function fails, the listener's onFailure handler will be called. The principle is that the mapped listener will handle
     * exceptions from the mapping function {@code fn} but it is the responsibility of {@code delegate} to handle its own exceptions
     * inside `onResponse` and `onFailure`.
     *
     * @param fn Function to apply to listener response
     * @param <T> Response type of the wrapped listener
     * @return a listener that maps the received response and then passes it to this instance
     */
    default <T> ActionListener<T> map(CheckedFunction<T, Response, Exception> fn) {
        return new MappedActionListener<>(fn, this);
    }

    abstract class Delegating<Response, DelegateResponse> implements ActionListener<Response> {

        protected final ActionListener<DelegateResponse> delegate;

        protected Delegating(ActionListener<DelegateResponse> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void onFailure(Exception e) {
            try {
                delegate.onFailure(e);
            } catch (RuntimeException ex) {
                if (ex != e) {
                    ex.addSuppressed(e);
                }
                assert false : new AssertionError("listener.onFailure failed", ex);
                throw ex;
            }
        }

        @Override
        public String toString() {
            return getClass().getName() + "/" + delegate;
        }
    }

    final class MappedActionListener<Response, MappedResponse> extends Delegating<Response, MappedResponse> {

        private final CheckedFunction<Response, MappedResponse, Exception> fn;

        private MappedActionListener(CheckedFunction<Response, MappedResponse, Exception> fn, ActionListener<MappedResponse> delegate) {
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
                assert false : new AssertionError("map: listener.onResponse failed", e);
                throw e;
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
    }

    /**
     * Creates a listener that listens for a response (or failure) and executes the
     * corresponding consumer when the response (or failure) is received.
     *
     * @param onResponse the checked consumer of the response, when the listener receives one
     * @param onFailure the consumer of the failure, when the listener receives one
     * @param <Response> the type of the response
     * @return a listener that listens for responses and invokes the consumer when received
     */
    static <Response> ActionListener<Response> wrap(
        CheckedConsumer<Response, ? extends Exception> onResponse,
        Consumer<Exception> onFailure
    ) {
        return new ActionListener<Response>() {
            @Override
            public void onResponse(Response response) {
                try {
                    onResponse.accept(response);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                onFailure.accept(e);
            }

            @Override
            public String toString() {
                return "WrappedActionListener{" + onResponse + "}{" + onFailure + "}";
            }
        };
    }

    /**
     * Creates a listener that delegates all responses it receives to this instance.
     *
     * @param bc BiConsumer invoked with delegate listener and exception
     * @return Delegating listener
     */
    default ActionListener<Response> delegateResponse(BiConsumer<ActionListener<Response>, Exception> bc) {
        return new DelegatingActionListener<>(this, bc);
    }

    /**
     * Creates a listener that delegates all exceptions it receives to another listener.
     *
     * @param bc BiConsumer invoked with delegate listener and response
     * @param <T> Type of the delegating listener's response
     * @return Delegating listener
     */
    default <T> ActionListener<T> delegateFailure(BiConsumer<ActionListener<Response>, T> bc) {
        return new DelegatingFailureActionListener<>(this, bc);
    }

    final class DelegatingActionListener<T> extends Delegating<T, T> {

        private final BiConsumer<ActionListener<T>, Exception> bc;

        DelegatingActionListener(ActionListener<T> delegate, BiConsumer<ActionListener<T>, Exception> bc) {
            super(delegate);
            this.bc = bc;
        }

        @Override
        public void onResponse(T t) {
            delegate.onResponse(t);
        }

        @Override
        public void onFailure(Exception e) {
            try {
                bc.accept(delegate, e);
            } catch (RuntimeException ex) {
                if (ex != e) {
                    ex.addSuppressed(e);
                }
                assert false : new AssertionError("listener.onFailure failed", ex);
                throw ex;
            }
        }

        @Override
        public String toString() {
            return super.toString() + "/" + bc;
        }
    }

    final class DelegatingFailureActionListener<T, R> extends Delegating<T, R> {

        private final BiConsumer<ActionListener<R>, T> bc;

        DelegatingFailureActionListener(ActionListener<R> delegate, BiConsumer<ActionListener<R>, T> bc) {
            super(delegate);
            this.bc = bc;
        }

        @Override
        public void onResponse(T t) {
            bc.accept(delegate, t);
        }

        @Override
        public String toString() {
            return super.toString() + "/" + bc;
        }
    }

    /**
     * Creates a listener which releases the given resource on completion (whether success or failure)
     */
    static <Response> ActionListener<Response> releasing(Releasable releasable) {
        return assertOnce(wrap(runnableFromReleasable(releasable)));
    }

    /**
     * Creates a listener that listens for a response (or failure) and executes the
     * corresponding runnable when the response (or failure) is received.
     *
     * @param runnable the runnable that will be called in event of success or failure
     * @param <Response> the type of the response
     * @return a listener that listens for responses and invokes the runnable when received
     */
    static <Response> ActionListener<Response> wrap(Runnable runnable) {
        return new ActionListener<>() {
            @Override
            public void onResponse(Response response) {
                try {
                    runnable.run();
                } catch (RuntimeException e) {
                    assert false : e;
                    throw e;
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    runnable.run();
                } catch (RuntimeException ex) {
                    ex.addSuppressed(e);
                    assert false : ex;
                    throw ex;
                }
            }

            @Override
            public String toString() {
                return "RunnableWrappingActionListener{" + runnable + "}";
            }
        };
    }

    /**
     * Adds a wrapper around a listener which catches exceptions thrown by its {@link #onResponse} method and feeds them to its
     * {@link #onFailure}.
     */
    static <DelegateResponse, Response extends DelegateResponse> ActionListener<Response> wrap(ActionListener<DelegateResponse> delegate) {
        return new ActionListener<>() {
            @Override
            public void onResponse(Response response) {
                ActionListener.run(delegate, l -> l.onResponse(response));
            }

            @Override
            public void onFailure(Exception e) {
                delegate.onFailure(e);
            }

            @Override
            public String toString() {
                return "wrapped{" + delegate + "}";
            }
        };
    }

    /**
     * Notifies every given listener with the response passed to {@link #onResponse(Object)}. If a listener itself throws an exception
     * the exception is forwarded to {@link #onFailure(Exception)}. If in turn {@link #onFailure(Exception)} fails all remaining
     * listeners will be processed and the caught exception will be re-thrown.
     */
    static <Response> void onResponse(Iterable<ActionListener<Response>> listeners, Response response) {
        List<Exception> exceptionList = new ArrayList<>();
        for (ActionListener<Response> listener : listeners) {
            try {
                listener.onResponse(response);
            } catch (Exception ex) {
                try {
                    listener.onFailure(ex);
                } catch (Exception ex1) {
                    exceptionList.add(ex1);
                }
            }
        }
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptionList);
    }

    /**
     * Notifies every given listener with the failure passed to {@link #onFailure(Exception)}. If a listener itself throws an exception
     * all remaining listeners will be processed and the caught exception will be re-thrown.
     */
    static <Response> void onFailure(Iterable<ActionListener<Response>> listeners, Exception failure) {
        List<Exception> exceptionList = new ArrayList<>();
        for (ActionListener<Response> listener : listeners) {
            try {
                listener.onFailure(failure);
            } catch (Exception ex) {
                exceptionList.add(ex);
            }
        }
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptionList);
    }

    /**
     * Wraps a given listener and returns a new listener which executes the provided {@code runAfter}
     * callback when the listener is notified via either {@code #onResponse} or {@code #onFailure}.
     */
    static <Response> ActionListener<Response> runAfter(ActionListener<Response> delegate, Runnable runAfter) {
        return assertOnce(new RunAfterActionListener<>(delegate, runAfter));
    }

    /**
     * Wraps a given listener and returns a new listener which releases the provided {@code releaseAfter}
     * resource when the listener is notified via either {@code #onResponse} or {@code #onFailure}.
     */
    static <Response> ActionListener<Response> releaseAfter(ActionListener<Response> delegate, Releasable releaseAfter) {
        return assertOnce(new RunAfterActionListener<>(delegate, runnableFromReleasable(releaseAfter)));
    }

    final class RunAfterActionListener<T> extends Delegating<T, T> {

        private final Runnable runAfter;

        protected RunAfterActionListener(ActionListener<T> delegate, Runnable runAfter) {
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

    /**
     * Wraps a given listener and returns a new listener which executes the provided {@code runBefore}
     * callback before the listener is notified via either {@code #onResponse} or {@code #onFailure}.
     * If the callback throws an exception then it will be passed to the listener's {@code #onFailure} and its {@code #onResponse} will
     * not be executed.
     */
    static <Response> ActionListener<Response> runBefore(ActionListener<Response> delegate, CheckedRunnable<?> runBefore) {
        return assertOnce(new RunBeforeActionListener<>(delegate, runBefore));
    }

    final class RunBeforeActionListener<T> extends Delegating<T, T> {

        private final CheckedRunnable<?> runBefore;

        protected RunBeforeActionListener(ActionListener<T> delegate, CheckedRunnable<?> runBefore) {
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

    /**
     * Wraps a given listener and returns a new listener which makes sure {@link #onResponse(Object)}
     * and {@link #onFailure(Exception)} of the provided listener will be called at most once.
     */
    static <Response> ActionListener<Response> notifyOnce(ActionListener<Response> delegate) {
        final var delegateRef = new AtomicReference<>(delegate);
        return new ActionListener<>() {
            @Override
            public void onResponse(Response response) {
                final var acquired = delegateRef.getAndSet(null);
                if (acquired != null) {
                    acquired.onResponse(response);
                }
            }

            @Override
            public void onFailure(Exception e) {
                final var acquired = delegateRef.getAndSet(null);
                if (acquired != null) {
                    acquired.onFailure(e);
                }
            }

            @Override
            public String toString() {
                return "notifyOnce[" + delegateRef.get() + "]";
            }
        };
    }

    /**
     * Completes the given listener with the result from the provided supplier accordingly.
     * This method is mainly used to complete a listener with a block of synchronous code.
     *
     * If the supplier fails, the listener's onFailure handler will be called.
     * It is the responsibility of {@code delegate} to handle its own exceptions inside `onResponse` and `onFailure`.
     */
    static <Response> void completeWith(ActionListener<Response> listener, CheckedSupplier<Response, ? extends Exception> supplier) {
        Response response;
        try {
            response = supplier.get();
        } catch (Exception e) {
            try {
                listener.onFailure(e);
            } catch (RuntimeException ex) {
                assert false : ex;
                throw ex;
            }
            return;
        }
        try {
            listener.onResponse(response);
        } catch (RuntimeException ex) {
            assert false : ex;
            throw ex;
        }
    }

    private static Runnable runnableFromReleasable(Releasable releasable) {
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

    static <Response> ActionListener<Response> assertOnce(ActionListener<Response> delegate) {
        if (Assertions.ENABLED) {
            return new ActionListener<>() {

                // if complete, records the stack trace which first completed it
                private final AtomicReference<ElasticsearchException> firstCompletion = new AtomicReference<>();

                private void assertFirstRun() {
                    var previousRun = firstCompletion.compareAndExchange(null, new ElasticsearchException(delegate.toString()));
                    assert previousRun == null : previousRun; // reports the stack traces of both completions
                }

                @Override
                public void onResponse(Response response) {
                    assertFirstRun();
                    delegate.onResponse(response);
                }

                @Override
                public void onFailure(Exception e) {
                    assertFirstRun();
                    delegate.onFailure(e);
                }

                @Override
                public String toString() {
                    return delegate.toString();
                }
            };
        } else {
            return delegate;
        }
    }

    static <T, L extends ActionListener<T>> void run(L listener, CheckedConsumer<L, Exception> action) {
        try {
            action.accept(listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

}
