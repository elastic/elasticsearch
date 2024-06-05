/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

/**
 * Represents a collection of global checkpoint listeners. This collection can be added to, and all listeners present at the time of an
 * update will be notified together. All listeners will be notified when the shard is closed.
 */
public class GlobalCheckpointListeners implements Closeable {

    /**
     * A global checkpoint listener consisting of a callback that is notified when the global checkpoint is updated or the shard is closed.
     */
    public interface GlobalCheckpointListener {

        /**
         * The executor on which the listener is notified.
         *
         * @return the executor
         */
        Executor executor();

        /**
         * Callback when the global checkpoint is updated or the shard is closed. If the shard is closed, the value of the global checkpoint
         * will be set to {@link org.elasticsearch.index.seqno.SequenceNumbers#UNASSIGNED_SEQ_NO} and the exception will be non-null and an
         * instance of {@link IndexShardClosedException }. If the listener timed out waiting for notification then the exception will be
         * non-null and an instance of {@link TimeoutException}. If the global checkpoint is updated, the exception will be null.
         *
         * @param globalCheckpoint the updated global checkpoint
         * @param e                if non-null, the shard is closed or the listener timed out
         */
        void accept(long globalCheckpoint, Exception e);

    }

    // guarded by this
    private boolean closed;
    private final Map<GlobalCheckpointListener, Tuple<Long, ScheduledFuture<?>>> listeners = new LinkedHashMap<>();
    private long lastKnownGlobalCheckpoint = UNASSIGNED_SEQ_NO;

    private final ShardId shardId;
    private final ScheduledExecutorService scheduler;
    private final Logger logger;

    /**
     * Construct a global checkpoint listeners collection.
     *
     * @param shardId   the shard ID on which global checkpoint updates can be listened to
     * @param scheduler the executor used for scheduling timeouts
     * @param logger    a shard-level logger
     */
    GlobalCheckpointListeners(final ShardId shardId, final ScheduledExecutorService scheduler, final Logger logger) {
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.logger = Objects.requireNonNull(logger, "logger");
    }

    /**
     * Add a global checkpoint listener. If the global checkpoint is equal to or above the global checkpoint the listener is waiting for,
     * then the listener will be asynchronously notified on the executor used to construct this collection of global checkpoint listeners.
     * If the shard is closed then the listener will be asynchronously notified on the executor used to construct this collection of global
     * checkpoint listeners. The listener will only be notified of at most one event, either the global checkpoint is updated above the
     * global checkpoint the listener is waiting for, or the shard is closed. A listener must re-register after one of these events to
     * receive subsequent events. Callers may add a timeout to be notified after if the timeout elapses. In this case, the listener will be
     * notified with a {@link TimeoutException}. Passing null fo the timeout means no timeout will be associated to the listener.
     *
     * @param waitingForGlobalCheckpoint the current global checkpoint known to the listener
     * @param listener                   the listener
     * @param timeout                    the listener timeout, or null if no timeout
     */
    synchronized void add(final long waitingForGlobalCheckpoint, final GlobalCheckpointListener listener, final TimeValue timeout) {
        if (closed) {
            notifyListener(listener, UNASSIGNED_SEQ_NO, new IndexShardClosedException(shardId));
            return;
        }
        if (lastKnownGlobalCheckpoint >= waitingForGlobalCheckpoint) {
            // notify directly
            notifyListener(listener, lastKnownGlobalCheckpoint, null);
        } else {
            if (timeout == null) {
                listeners.put(listener, Tuple.tuple(waitingForGlobalCheckpoint, null));
            } else {
                listeners.put(listener, Tuple.tuple(waitingForGlobalCheckpoint, scheduler.schedule(() -> {
                    final boolean removed;
                    synchronized (this) {
                        /*
                         * We know that this listener has a timeout associated with it (otherwise we would not be
                         * here) so the future component of the return value from remove being null is an indication
                         * that we are not in the map. This can happen if a notification collected us into listeners
                         * to be notified and removed us from the map, and then our scheduled execution occurred
                         * before we could be cancelled by the notification. In this case, our listener here would
                         * not be in the map and we should not fire the timeout logic.
                         */
                        removed = listeners.remove(listener) != null;
                    }
                    if (removed) {
                        final TimeoutException e = new TimeoutException(timeout.getStringRep());
                        logger.trace("global checkpoint listener timed out", e);
                        notifyListener(listener, UNASSIGNED_SEQ_NO, e);
                    }
                }, timeout.nanos(), TimeUnit.NANOSECONDS)));
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            assert listeners.isEmpty() : listeners;
        }
        closed = true;
        notifyListeners(UNASSIGNED_SEQ_NO, new IndexShardClosedException(shardId));
    }

    /**
     * The number of listeners currently pending for notification.
     *
     * @return the number of listeners pending notification
     */
    synchronized int pendingListeners() {
        return listeners.size();
    }

    /**
     * The scheduled future for a listener that has a timeout associated with it, otherwise null.
     *
     * @param listener the listener to get the scheduled future for
     * @return a scheduled future representing the timeout future for the listener, otherwise null
     */
    synchronized ScheduledFuture<?> getTimeoutFuture(final GlobalCheckpointListener listener) {
        return listeners.get(listener).v2();
    }

    /**
     * Invoke to notify all registered listeners of an updated global checkpoint.
     *
     * @param globalCheckpoint the updated global checkpoint
     */
    synchronized void globalCheckpointUpdated(final long globalCheckpoint) {
        assert globalCheckpoint >= NO_OPS_PERFORMED;
        assert globalCheckpoint > lastKnownGlobalCheckpoint
            : "updated global checkpoint ["
                + globalCheckpoint
                + "]"
                + " is not more than the last known global checkpoint ["
                + lastKnownGlobalCheckpoint
                + "]";
        lastKnownGlobalCheckpoint = globalCheckpoint;
        notifyListeners(globalCheckpoint, null);
    }

    private void notifyListeners(final long globalCheckpoint, final IndexShardClosedException e) {
        assert Thread.holdsLock(this) : Thread.currentThread();

        // early return if there are no listeners
        if (listeners.isEmpty()) {
            return;
        }

        final Map<GlobalCheckpointListener, Tuple<Long, ScheduledFuture<?>>> listenersToNotify;
        if (globalCheckpoint != UNASSIGNED_SEQ_NO) {
            listenersToNotify = listeners.entrySet()
                .stream()
                .filter(entry -> entry.getValue().v1() <= globalCheckpoint)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            listenersToNotify.keySet().forEach(listeners::remove);
        } else {
            listenersToNotify = new HashMap<>(listeners);
            listeners.clear();
        }
        if (listenersToNotify.isEmpty() == false) {
            listenersToNotify.forEach((listener, t) -> {
                /*
                 * We do not want to interrupt any timeouts that fired, these will detect that the listener has been notified and not
                 * trigger the timeout.
                 */
                FutureUtils.cancel(t.v2());
                notifyListener(listener, globalCheckpoint, e);
            });
        }
    }

    private void notifyListener(final GlobalCheckpointListener listener, final long globalCheckpoint, final Exception e) {
        assertNotification(globalCheckpoint, e);

        listener.executor().execute(() -> {
            try {
                listener.accept(globalCheckpoint, e);
            } catch (final Exception caught) {
                if (globalCheckpoint != UNASSIGNED_SEQ_NO) {
                    logger.warn(
                        () -> format("error notifying global checkpoint listener of updated global checkpoint [%s]", globalCheckpoint),
                        caught
                    );
                } else if (e instanceof IndexShardClosedException) {
                    logger.warn("error notifying global checkpoint listener of closed shard", caught);
                } else {
                    logger.warn("error notifying global checkpoint listener of timeout", caught);
                }
            }
        });
    }

    private static void assertNotification(final long globalCheckpoint, final Exception e) {
        if (Assertions.ENABLED) {
            assert globalCheckpoint >= UNASSIGNED_SEQ_NO : globalCheckpoint;
            if (globalCheckpoint != UNASSIGNED_SEQ_NO) {
                assert e == null : e;
            } else {
                assert e != null;
                assert e instanceof IndexShardClosedException || e instanceof TimeoutException : e;
            }
        }
    }

}
