/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.transport.LeakTracker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.action.ActionListenerImplementations.runnableFromReleasable;
import static org.elasticsearch.action.ActionListenerImplementations.safeAcceptException;
import static org.elasticsearch.action.ActionListenerImplementations.safeOnFailure;

/**
 * <p>
 * Callbacks are used extensively throughout Elasticsearch because they enable us to write asynchronous and nonblocking code, i.e. code
 * which doesn't necessarily compute a result straight away but also doesn't block the calling thread waiting for the result to become
 * available. They support several useful control flows:
 * </p>
 * <ul>
 * <li>They can be completed immediately on the calling thread.</li>
 * <li>They can be completed concurrently on a different thread.</li>
 * <li>They can be stored in a data structure and completed later on when the system reaches a particular state.</li>
 * <li>Most commonly, they can be passed on to other methods that themselves require a callback.</li>
 * <li>They can be wrapped in another callback which modifies the behaviour of the original callback, perhaps adding some extra code to run
 * before or after completion, before passing them on.</li>
 * </ul>
 * <p>
 * {@link ActionListener} is a general-purpose callback interface that is used extensively across the Elasticsearch codebase. {@link
 * ActionListener} is used pretty much everywhere that needs to perform some asynchronous and nonblocking computation. The uniformity makes
 * it easier to compose parts of the system together without needing to build adapters to convert back and forth between different kinds of
 * callback. It also makes it easier to develop the skills needed to read and understand all the asynchronous code, although this definitely
 * takes practice and is certainly not easy in an absolute sense. Finally, it has allowed us to build a rich library for working with {@link
 * ActionListener} instances themselves, creating new instances out of existing ones and completing them in interesting ways. See for
 * instance:
 * </p>
 * <ul>
 * <li>All the static methods on {@link ActionListener} itself.</li>
 * <li>{@link org.elasticsearch.action.support.ThreadedActionListener} for forking work elsewhere.</li>
 * <li>{@link org.elasticsearch.action.support.RefCountingListener} for running work in parallel.</li>
 * <li>{@link org.elasticsearch.action.support.SubscribableListener} for constructing flexible workflows.</li>
 * </ul>
 * <p>
 * Callback-based asynchronous code can easily call regular synchronous code, but synchronous code cannot run callback-based asynchronous
 * code without blocking the calling thread until the callback is called back. This blocking is at best undesirable (threads are too
 * expensive to waste with unnecessary blocking) and at worst outright broken (the blocking can lead to deadlock). Unfortunately this means
 * that most of our code ends up having to be written with callbacks, simply because it's ultimately calling into some other code that takes
 * a callback. The entry points for all Elasticsearch APIs are callback-based (e.g. REST APIs all start at {@link
 * org.elasticsearch.rest.BaseRestHandler}{@code #prepareRequest} and transport APIs all start at {@link
 * org.elasticsearch.action.support.TransportAction}{@code #doExecute} and the whole system fundamentally works in terms of an event loop
 * (an {@code io.netty.channel.EventLoop}) which processes network events via callbacks.
 * </p>
 * <p>
 * {@link ActionListener} is not an <i>ad-hoc</i> invention. Formally speaking, it is our implementation of the general concept of a
 * continuation in the sense of <a href="https://en.wikipedia.org/wiki/Continuation-passing_style"><i>continuation-passing style</i></a>
 * (CPS): an extra argument to a function which defines how to continue the computation when the result is available. This is in contrast to
 * <i>direct style</i> which is the more usual style of calling methods that return values directly back to the caller so they can continue
 * executing as normal. There's essentially two ways that computation can continue in Java (it can return a value or it can throw an
 * exception) which is why {@link ActionListener} has both an {@link #onResponse} and an {@link #onFailure} method.
 * </p>
 * <p>
 * CPS is strictly more expressive than direct style: direct code can be mechanically translated into continuation-passing style, but CPS
 * also enables all sorts of other useful control structures such as forking work onto separate threads, possibly to be executed in
 * parallel, perhaps even across multiple nodes, or possibly collecting a list of continuations all waiting for the same condition to be
 * satisfied before proceeding (e.g. {@link org.elasticsearch.action.support.SubscribableListener} amongst many others). Some languages have
 * first-class support for continuations (e.g. the {@code async} and {@code await} primitives in C#) allowing the programmer to write code
 * in direct style away from those exotic control structures, but Java does not. That's why we have to manipulate all the callbacks
 * ourselves.
 * </p>
 * <p>
 * Strictly speaking, CPS requires that a computation <i>only</i> continues by calling the continuation. In Elasticsearch, this means that
 * asynchronous methods must have {@code void} return type and may not throw any exceptions. This is mostly the case in our code as written
 * today, and is a good guiding principle, but we don't enforce void exceptionless methods and there are some deviations from this rule. In
 * particular, it's not uncommon to permit some methods to throw an exception, using things like {@link ActionListener#run} (or an
 * equivalent {@code try ... catch ...} block) further up the stack to handle it. Some methods also take (and may complete) an {@link
 * ActionListener} parameter, but still return a value separately for other local synchronous work.
 * </p>
 * <p>
 * This pattern is often used in the transport action layer with the use of the {@link
 * org.elasticsearch.action.support.ChannelActionListener} class, which wraps a {@link org.elasticsearch.transport.TransportChannel}
 * produced by the transport layer.{@link org.elasticsearch.transport.TransportChannel} implementations can hold a reference to a Netty
 * channel with which to pass the response back to the network caller. Netty has a many-to-one association of network callers to channels,
 * so a call taking a long time generally won't hog resources: it's cheap. A transport action can take hours to respond and that's alright,
 * barring caller timeouts.
 * </p>
 * <p>
 * Note that we explicitly avoid {@link java.util.concurrent.CompletableFuture} and other similar mechanisms as much as possible. They
 * can achieve the same goals as {@link ActionListener}, but can also easily be misused in various ways that lead to severe bugs. In
 * particular, futures support blocking while waiting for a result, but this is almost never appropriate in Elasticsearch's production code
 * where threads are such a precious resource. Moreover if something throws an {@link Error} then the JVM should exit pretty much straight
 * away, but {@link java.util.concurrent.CompletableFuture} can catch an {@link Error} which delays the JVM exit until its result is
 * observed. This may be much later, or possibly even never. It's not possible to introduce such bugs when using {@link ActionListener}.
 * </p>
 */
public interface ActionListener<Response> {
    /**
     * Complete this listener with a successful (or at least, non-exceptional) response.
     */
    void onResponse(Response response);

    /**
     * Complete this listener with an exceptional response.
     */
    void onFailure(Exception e);

    /**
     * @return a listener that does nothing
     */
    @SuppressWarnings("unchecked")
    static <T> ActionListener<T> noop() {
        return (ActionListener<T>) ActionListenerImplementations.NOOP;
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
        return new ActionListenerImplementations.MappedActionListener<>(fn, this);
    }

    /**
     * Same as {@link #map(CheckedFunction)} except that {@code fn} is expected to never throw.
     */
    default <T> ActionListener<T> safeMap(Function<T, Response> fn) {
        return new ActionListenerImplementations.SafeMappedActionListener<>(fn, this);
    }

    /**
     * Creates a listener that delegates all responses it receives to this instance.
     *
     * @param bc BiConsumer invoked with delegate listener and exception
     * @return Delegating listener
     */
    default ActionListener<Response> delegateResponse(BiConsumer<ActionListener<Response>, Exception> bc) {
        return new ActionListenerImplementations.DelegatingResponseActionListener<>(this, bc);
    }

    /**
     * Creates a new listener, wrapping this one, that overrides {@link #onResponse} handling with the given {@code bc} consumer.
     * {@link #onFailure(Exception)} handling is delegated to the original listener. Exceptions in {@link #onResponse} are forbidden.
     *
     * @param bc {@link BiConsumer} invoked via {@link #onResponse} with the original listener and the response with which the new listener
     * was completed.
     * @param <T> Type of the delegating listener's response
     * @return a new listener that delegates failures to this listener and runs {@code bc} on a response.
     */
    default <T> ActionListener<T> delegateFailure(BiConsumer<ActionListener<Response>, T> bc) {
        return new ActionListenerImplementations.DelegatingFailureActionListener<>(this, bc);
    }

    /**
     * Same as {@link #delegateFailure(BiConsumer)} except that any failure thrown by {@code bc} or the original listener's
     * {@link #onResponse} will be passed to the original listener's {@link #onFailure(Exception)}.
     */
    default <T> ActionListener<T> delegateFailureAndWrap(CheckedBiConsumer<ActionListener<Response>, T, ? extends Exception> bc) {
        return new ActionListenerImplementations.ResponseWrappingActionListener<>(this, bc);
    }

    /**
     * Same as {@link #delegateFailureAndWrap(CheckedBiConsumer)} except that the response is ignored and not passed to the delegate.
     */
    default <T> ActionListener<T> delegateFailureIgnoreResponseAndWrap(CheckedConsumer<ActionListener<Response>, ? extends Exception> c) {
        return new ActionListenerImplementations.ResponseDroppingActionListener<>(this, c);
    }

    /**
     * Creates a listener which releases the given resource on completion (whether success or failure)
     */
    static <Response> ActionListener<Response> releasing(Releasable releasable) {
        return assertOnce(running(runnableFromReleasable(releasable)));
    }

    /**
     * Creates a listener that executes the given runnable on completion (whether successful or otherwise).
     *
     * @param runnable the runnable that will be called in event of success or failure. This must not throw.
     * @param <Response> the type of the response, which is ignored.
     * @return a listener that executes the given runnable on completion (whether successful or otherwise).
     */
    static <Response> ActionListener<Response> running(Runnable runnable) {
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
                safeAcceptException(ignored -> runnable.run(), e);
            }

            @Override
            public String toString() {
                return "RunnableWrappingActionListener{" + runnable + "}";
            }
        };
    }

    /**
     * Creates a listener that executes the appropriate consumer when the response (or failure) is received. This listener is "wrapped" in
     * the sense that an exception from the {@code onResponse} consumer is passed into the {@code onFailure} consumer.
     * <p>
     * If the {@code onFailure} argument is {@code listener::onFailure} for some other {@link ActionListener}, prefer to use
     * {@link #delegateFailureAndWrap} instead for performance reasons.
     * @param onResponse the checked consumer of the response, executed when the listener is completed successfully. If it throws an
     *                   exception, the exception is passed to the {@code onFailure} consumer.
     * @param onFailure the consumer of the failure, executed when the listener is completed with an exception (or it is completed
     *                  successfully but the {@code onResponse} consumer threw an exception).
     * @param <Response> the type of the response
     * @return a listener that executes the appropriate consumer when the response (or failure) is received.
     */
    static <Response> ActionListener<Response> wrap(
        CheckedConsumer<Response, ? extends Exception> onResponse,
        Consumer<Exception> onFailure
    ) {
        return new ActionListener<>() {
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
                safeAcceptException(onFailure, e);
            }

            @Override
            public String toString() {
                return "WrappedActionListener{" + onResponse + "}{" + onFailure + "}";
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
                    safeOnFailure(listener, ex);
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
        return assertOnce(new ActionListenerImplementations.RunAfterActionListener<>(delegate, runAfter));
    }

    /**
     * Wraps a given listener and returns a new listener which releases the provided {@code releaseAfter}
     * resource when the listener is notified via either {@code #onResponse} or {@code #onFailure}.
     */
    static <Response> ActionListener<Response> releaseAfter(ActionListener<Response> delegate, Releasable releaseAfter) {
        return assertOnce(new ActionListenerImplementations.RunAfterActionListener<>(delegate, runnableFromReleasable(releaseAfter)));
    }

    /**
     * Wraps a given listener and returns a new listener which executes the provided {@code runBefore}
     * callback before the listener is notified via either {@code #onResponse} or {@code #onFailure}.
     * If the callback throws an exception then it will be passed to the listener's {@code #onFailure} and its {@code #onResponse} will
     * not be executed.
     */
    static <Response> ActionListener<Response> runBefore(ActionListener<Response> delegate, CheckedRunnable<?> runBefore) {
        return assertOnce(new ActionListenerImplementations.RunBeforeActionListener<>(delegate, runBefore));
    }

    /**
     * Wraps a given listener and returns a new listener which makes sure {@link #onResponse(Object)}
     * and {@link #onFailure(Exception)} of the provided listener will be called at most once.
     */
    static <Response> ActionListener<Response> notifyOnce(ActionListener<Response> delegate) {
        return new ActionListenerImplementations.NotifyOnceActionListener<>(delegate);
    }

    /**
     * Completes the given listener with the result from the provided supplier accordingly.
     * This method is mainly used to complete a listener with a block of synchronous code.
     *
     * If the supplier fails, the listener's onFailure handler will be called.
     * It is the responsibility of {@code delegate} to handle its own exceptions inside `onResponse` and `onFailure`.
     */
    static <Response> void completeWith(ActionListener<Response> listener, CheckedSupplier<Response, ? extends Exception> supplier) {
        final Response response;
        try {
            response = supplier.get();
        } catch (Exception e) {
            safeOnFailure(listener, e);
            return;
        }
        try {
            listener.onResponse(response);
        } catch (RuntimeException ex) {
            assert false : ex;
            throw ex;
        }
    }

    /**
     * Shorthand for resolving given {@code listener} with given {@code response} and decrementing the response's ref count by one
     * afterwards.
     */
    static <R extends RefCounted> void respondAndRelease(ActionListener<R> listener, R response) {
        try {
            listener.onResponse(response);
        } finally {
            response.decRef();
        }
    }

    /**
     * @return A listener which (if assertions are enabled) wraps around the given delegate and asserts that it is only called once.
     */
    static <Response> ActionListener<Response> assertOnce(ActionListener<Response> delegate) {
        if (Assertions.ENABLED) {
            return new ActionListener<>() {

                // if complete, records the stack trace which first completed it
                private final AtomicReference<ElasticsearchException> firstCompletion = new AtomicReference<>();

                private void assertFirstRun() {
                    var previousRun = firstCompletion.compareAndExchange(null, new ElasticsearchException("executed already"));
                    assert previousRun == null : "[" + delegate + "] " + previousRun; // reports the stack traces of both completions
                }

                @Override
                public void onResponse(Response response) {
                    assertFirstRun();
                    try {
                        delegate.onResponse(response);
                    } catch (Exception e) {
                        assert false : new AssertionError("listener [" + delegate + "] must handle its own exceptions", e);
                        throw e;
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    assertFirstRun();
                    safeOnFailure(delegate, e);
                }

                @Override
                public String toString() {
                    return delegate.toString();
                }

                @Override
                public int hashCode() {
                    // It's legitimate to wrap the delegate twice, with two different assertOnce calls, which would yield different objects
                    // if and only if assertions are enabled. So we'd better not ever use these things as map keys etc.
                    throw new AssertionError("almost certainly a mistake to need the hashCode() of a one-shot ActionListener");
                }

                @Override
                public boolean equals(Object obj) {
                    // It's legitimate to wrap the delegate twice, with two different assertOnce calls, which would yield different objects
                    // if and only if assertions are enabled. So we'd better not ever use these things as map keys etc.
                    throw new AssertionError("almost certainly a mistake to compare a one-shot ActionListener for equality");
                }
            };
        } else {
            return delegate;
        }
    }

    /**
     * @return A listener which (if assertions are enabled) wraps around the given delegate and asserts that it is called at least once.
     */
    static <Response> ActionListener<Response> assertAtLeastOnce(ActionListener<Response> delegate) {
        if (Assertions.ENABLED) {
            return new ActionListenerImplementations.RunBeforeActionListener<>(delegate, LeakTracker.INSTANCE.track(delegate)::close);
        }
        return delegate;
    }

    /**
     * Execute the given action in a {@code try/catch} block which feeds all exceptions to the given listener's {@link #onFailure} method.
     */
    static <T, L extends ActionListener<T>> void run(L listener, CheckedConsumer<L, ? extends Exception> action) {
        try {
            action.accept(listener);
        } catch (Exception e) {
            safeOnFailure(listener, e);
        }
    }

    /**
     * Execute the given action in an (async equivalent of a) try-with-resources block which closes the supplied resource on completion, and
     * feeds all exceptions to the given listener's {@link #onFailure} method.
     */
    static <T, R extends AutoCloseable> void runWithResource(
        ActionListener<T> listener,
        CheckedSupplier<R, ? extends Exception> resourceSupplier,
        CheckedBiConsumer<ActionListener<T>, R, ? extends Exception> action
    ) {
        R resource;
        try {
            resource = resourceSupplier.get();
        } catch (Exception e) {
            safeOnFailure(listener, e);
            return;
        }

        ActionListener.run(ActionListener.runBefore(listener, resource::close), l -> action.accept(l, resource));
    }

    /**
     * Increments ref count and returns a listener that will decrement ref count on listener completion.
     */
    static <Response> ActionListener<Response> withRef(ActionListener<Response> listener, RefCounted ref) {
        ref.mustIncRef();
        return releaseAfter(listener, ref::decRef);
    }

}
