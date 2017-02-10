/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

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
     * @see #checkAndPublish(RestClient)
     */
    public boolean isDirty() {
        return state.get() != State.CLEAN;
    }

    /**
     * Mark the resource as {@linkplain #isDirty() dirty}.
     */
    public final void markDirty() {
        state.compareAndSet(State.CLEAN, State.DIRTY);
    }

    /**
     * If the resource is currently {@linkplain #isDirty() dirty}, then check and, if necessary, publish this {@link HttpResource}.
     * <p>
     * Expected usage:
     * <pre><code>
     * if (resource.checkAndPublishIfDirty(client)) {
     *     // use client with resources having been verified
     * }
     * </code></pre>
     *
     * @param client The REST client to make the request(s).
     * @return {@code true} if the resource is available for use. {@code false} to stop.
     */
    public final boolean checkAndPublishIfDirty(final RestClient client) {
        final State state = this.state.get();

        // get in line and wait until the check passes or fails if it's checking now, or start checking
        return state == State.CLEAN || blockUntilCheckAndPublish(client);
    }

    /**
     * Invoked by {@link #checkAndPublishIfDirty(RestClient)} to block incase {@link #checkAndPublish(RestClient)} is in the middle of
     * {@linkplain State#CHECKING checking}.
     * <p>
     * Unlike {@link #isDirty()} and {@link #checkAndPublishIfDirty(RestClient)}, this is {@code synchronized} in order to prevent
     * double-execution and it invokes {@link #checkAndPublish(RestClient)} if it's {@linkplain State#DIRTY dirty}.
     *
     * @param client The REST client to make the request(s).
     * @return {@code true} if the resource is available for use. {@code false} to stop.
     */
    private synchronized boolean blockUntilCheckAndPublish(final RestClient client) {
        final State state = this.state.get();

        return state == State.CLEAN || (state == State.DIRTY && checkAndPublish(client));
    }

    /**
     * Check and, if necessary, publish this {@link HttpResource}.
     * <p>
     * This will perform the check regardless of the {@linkplain #isDirty() dirtiness} and it will update the dirtiness.
     * Using this directly can be useful if there is ever a need to double-check dirtiness without having to {@linkplain #markDirty() mark}
     * it as dirty.
     *
     * @param client The REST client to make the request(s).
     * @return {@code true} if the resource is available for use. {@code false} to stop.
     * @see #isDirty()
     */
    public final synchronized boolean checkAndPublish(final RestClient client) {
        // we always check when asked, regardless of clean or dirty
        state.set(State.CHECKING);

        boolean success = false;

        try {
            success = doCheckAndPublish(client);
        } finally {
            // nothing else should be unsetting from CHECKING
            assert state.get() == State.CHECKING;

            state.set(success ? State.CLEAN : State.DIRTY);
        }

        return success;
    }

    /**
     * Perform whatever is necessary to check and publish this {@link HttpResource}.
     *
     * @param client The REST client to make the request(s).
     * @return {@code true} if the resource is available for use. {@code false} to stop.
     */
    protected abstract boolean doCheckAndPublish(RestClient client);

}
