/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.translog.Translog;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class FlushListeners implements Closeable {

    private final Logger logger;
    private final ThreadContext threadContext;

    private volatile Tuple<Long, Translog.Location> lastCommit;

    /**
     * Is this closed? If true then we won't add more listeners and have flushed all pending listeners.
     */
    private volatile boolean closed = false;
    private volatile List<Tuple<Translog.Location, ActionListener<Long>>> locationCommitListeners = null;

    public FlushListeners(final Logger logger, final ThreadContext threadContext) {
        this.logger = logger;
        this.threadContext = threadContext;
    }

    public void addOrNotify(Translog.Location location, ActionListener<Long> listener) {
        requireNonNull(listener, "listener cannot be null");
        requireNonNull(location, "location cannot be null");

        Tuple<Long, Translog.Location> lastCommitBeforeSynchronized = lastCommit;
        if (lastCommitBeforeSynchronized != null && lastCommitBeforeSynchronized.v2().compareTo(location) >= 0) {
            // Location already visible, just call the listener
            listener.onResponse(lastCommitBeforeSynchronized.v1());
            return;
        }
        synchronized (this) {
            if (closed) {
                throw new IllegalStateException("can't wait for flush on a closed index");
            }
            Tuple<Long, Translog.Location> lastCommitAfterSynchronized = lastCommit;
            if (lastCommitAfterSynchronized != null && lastCommitAfterSynchronized.v2().compareTo(location) >= 0) {
                // Location already visible, just call the listener
                listener.onResponse(lastCommitAfterSynchronized.v1());
                return;
            }

            List<Tuple<Translog.Location, ActionListener<Long>>> listeners = locationCommitListeners;
            ActionListener<Long> contextPreservingListener = ContextPreservingActionListener.wrapPreservingContext(listener, threadContext);
            if (listeners == null) {
                listeners = new ArrayList<>();
            }
            // We have a free slot so register the listener
            listeners.add(new Tuple<>(location, contextPreservingListener));
            locationCommitListeners = listeners;
        }
    }

    public void afterFlush(final long generation, final Translog.Location lastCommitLocation) {
        this.lastCommit = new Tuple<>(generation, lastCommitLocation);

        List<Tuple<Translog.Location, ActionListener<Long>>> listenersToFire = null;
        List<Tuple<Translog.Location, ActionListener<Long>>> listenersToReregister = null;
        synchronized (this) {
            // No listeners to check so just bail early
            if (locationCommitListeners == null) {
                return;
            }

            for (Tuple<Translog.Location, ActionListener<Long>> tuple : locationCommitListeners) {
                Translog.Location location = tuple.v1();
                if (location.compareTo(lastCommitLocation) <= 0) {
                    if (listenersToFire == null) {
                        listenersToFire = new ArrayList<>();
                    }
                    listenersToFire.add(tuple);
                } else {
                    if (listenersToReregister == null) {
                        listenersToReregister = new ArrayList<>();
                    }
                    listenersToReregister.add(tuple);
                }
            }

            locationCommitListeners = listenersToReregister;
        }

        fireListeners(generation, listenersToFire);
    }

    private void fireListeners(final long generation, final List<Tuple<Translog.Location, ActionListener<Long>>> listenersToFire) {
        if (listenersToFire != null) {
            for (final Tuple<Translog.Location, ActionListener<Long>> listener : listenersToFire) {
                try {
                    listener.v2().onResponse(generation);
                } catch (final Exception e) {
                    logger.warn("error firing location refresh listener", e);
                }
            }
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed == false) {
                closed = true;
                if (locationCommitListeners != null) {
                    for (final Tuple<Translog.Location, ActionListener<Long>> listener : locationCommitListeners) {
                        try {
                            listener.v2().onFailure(new AlreadyClosedException("shard is closed"));
                        } catch (final Exception e) {
                            logger.warn("error firing checkpoint refresh listener", e);
                            assert false;
                        }
                    }
                    locationCommitListeners = null;
                }
            }
        }
    }
}
