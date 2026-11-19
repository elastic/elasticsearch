/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.core.Nullable;

/**
 * The source bytes of bulks on their way to the destination, summed over the drivers that share it, so that many drivers
 * cannot flood the destination together. The limit is soft: a driver that sees room may add a page that goes over it.
 */
final class InFlightBudget {
    private final long limit;
    private long inFlight; // guarded by this
    @Nullable
    private SubscribableListener<Void> waiting; // guarded by this

    InFlightBudget(long limit) {
        this.limit = limit;
    }

    synchronized void add(long bytes) {
        inFlight += bytes;
    }

    /** Returns {@code null} if there is room, and otherwise a listener that completes once there is. */
    @Nullable
    synchronized SubscribableListener<Void> awaitRoom() {
        if (inFlight < limit) {
            return null;
        }
        if (waiting == null) {
            waiting = new SubscribableListener<>();
        }
        return waiting;
    }

    void release(long bytes) {
        SubscribableListener<Void> room = null;
        synchronized (this) {
            inFlight -= bytes;
            if (inFlight < limit) {
                room = waiting;
                waiting = null;
            }
        }
        // Outside the monitor: the listeners wake drivers, which come back to ask for room
        if (room != null) {
            room.onResponse(null);
        }
    }
}
