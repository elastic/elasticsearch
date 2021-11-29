/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.RestClient;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An {@code HttpResource} is some "thing" that needs to exist on the other side. If it does not exist, then follow-on actions cannot
 * occur.
 * <p>
 * {@code HttpResource}s can assume that, as long as the connection stays active, then a verified resource should continue to exist on the
 * other side.
 *
 * @see MultiHttpResource
 * @see PublishableHttpResource
 */
public abstract class HttpResource {

    /**
     * The current state of the {@link HttpResource}.
     */
    enum State {

        /**
         * The resource is ready to use.
         */
        CLEAN,
        /**
         * The resource is being checked right now to see if it can be used.
         */
        CHECKING,
        /**
         * The resource needs to be checked before it can be used.
         */
        DIRTY
    }

    /**
     * Encapsulates the results of a check-and-publish operation, listing the operational state of the resource.
     */
    public static class ResourcePublishResult {
        private final boolean success;
        private final String reason;
        private final State resourceState;

        public ResourcePublishResult(boolean success) {
            this(success, null, success ? State.CLEAN : State.DIRTY);
        }

        public ResourcePublishResult(boolean success, String reason, State resourceState) {
            this.success = success;
            this.reason = reason;
            this.resourceState = resourceState;
        }

        /**
         * The publish operation succeeded without any problems.
         */
        public static ResourcePublishResult ready() {
            return new ResourcePublishResult(true, null, State.CLEAN);
        }

        /**
         * The publish operation succeeded without any problems.
         */
        public static ResourcePublishResult notReady(String reason) {
            return new ResourcePublishResult(false, reason, State.DIRTY);
        }

        /**
         * The publish operation was not attempted, since another publishing operation is already in flight.
         */
        public static ResourcePublishResult inProgress() {
            return new ResourcePublishResult(false, null, State.CHECKING);
        }

        public boolean isSuccess() {
            return success;
        }

        public String getReason() {
            return reason;
        }

        public State getResourceState() {
            return resourceState;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ResourcePublishResult that = (ResourcePublishResult) o;
            return success == that.success && Objects.equals(reason, that.reason) && resourceState == that.resourceState;
        }

        @Override
        public int hashCode() {
            return Objects.hash(success, reason, resourceState);
        }

        @Override
        public String toString() {
            return "ResourcePublishResult{"
                + "success="
                + success
                + ", reason='"
                + reason
                + '\''
                + ", resourceState="
                + resourceState
                + '}';
        }
    }

    /**
     * The user-recognizable name for whatever owns this {@link HttpResource}.
     */
    protected final String resourceOwnerName;
    /**
     * The current state of the resource, which helps to determine if it needs to be checked.
     */
    protected final AtomicReference<State> state;

    /**
     * Create a new {@link HttpResource} that {@linkplain #isDirty() is dirty}.
     *
     * @param resourceOwnerName The user-recognizable name
     */
    protected HttpResource(final String resourceOwnerName) {
        this(resourceOwnerName, true);
    }

    /**
     * Create a new {@link HttpResource} that is {@code dirty}.
     *
     * @param resourceOwnerName The user-recognizable name
     * @param dirty Whether the resource is dirty or not
     */
    protected HttpResource(final String resourceOwnerName, final boolean dirty) {
        this.resourceOwnerName = Objects.requireNonNull(resourceOwnerName);
        this.state = new AtomicReference<>(dirty ? State.DIRTY : State.CLEAN);
    }

    /**
     * Get the resource owner for this {@link HttpResource}.
     *
     * @return Never {@code null}.
     */
    public String getResourceOwnerName() {
        return resourceOwnerName;
    }

    /**
     * Determine if the resource needs to be checked.
     *
     * @return {@code true} to indicate that the resource should block follow-on actions that require it.
     * @see #checkAndPublish(RestClient, ActionListener)
     */
    public boolean isDirty() {
        return state.get() != State.CLEAN;
    }

    /**
     * Mark the resource as {@linkplain #isDirty() dirty}.
     */
    public final void markDirty() {
        state.set(State.DIRTY);
    }

    /**
     * If the resource is currently {@linkplain #isDirty() dirty}, then check and, if necessary, publish this {@link HttpResource}.
     * <p>
     * Expected usage:
     * <pre><code>
     * resource.checkAndPublishIfDirty(client, ActionListener.wrap((success) -&gt; {
     *     if (success) {
     *         // use client with resources having been verified
     *     }
     * }, listener::onFailure);
     * </code></pre>
     *
     * @param client The REST client to make the request(s).
     * @param listener Returns {@code true} if the resource is available for use. {@code false} to stop.
     */
    public final void checkAndPublishIfDirty(final RestClient client, final ActionListener<Boolean> listener) {
        if (state.get() == State.CLEAN) {
            listener.onResponse(true);
        } else {
            checkAndPublish(client, listener.map(ResourcePublishResult::isSuccess));
        }
    }

    /**
     * Check and, if necessary, publish this {@link HttpResource}.
     * <p>
     * This will perform the check regardless of the {@linkplain #isDirty() dirtiness} and it will update the dirtiness.
     * Using this directly can be useful if there is ever a need to double-check dirtiness without having to {@linkplain #markDirty() mark}
     * it as dirty.
     * <p>
     * If you do mark this as dirty while this is running (e.g., asynchronously something invalidates a resource), then the resource will
     * still be dirty at the end, but the success of it will still return based on the checks it ran.
     *
     * @param client The REST client to make the request(s).
     * @param listener Returns {@code true} if the resource is available for use. {@code false} to stop.
     * @see #isDirty()
     */
    public final void checkAndPublish(final RestClient client, final ActionListener<ResourcePublishResult> listener) {
        // we always check when asked, regardless of clean or dirty, but we do not run parallel checks
        if (state.getAndSet(State.CHECKING) != State.CHECKING) {
            doCheckAndPublish(client, ActionListener.wrap(publishResult -> {
                state.compareAndSet(State.CHECKING, publishResult.success ? State.CLEAN : State.DIRTY);
                listener.onResponse(publishResult);
            }, e -> {
                state.compareAndSet(State.CHECKING, State.DIRTY);
                listener.onFailure(e);
            }));
        } else {
            listener.onResponse(ResourcePublishResult.inProgress());
        }
    }

    /**
     * Perform whatever is necessary to check and publish this {@link HttpResource}.
     *
     * @param client The REST client to make the request(s).
     * @param listener Returns {@code true} if the resource is available for use. {@code false} to stop.
     */
    protected abstract void doCheckAndPublish(RestClient client, ActionListener<ResourcePublishResult> listener);

}
