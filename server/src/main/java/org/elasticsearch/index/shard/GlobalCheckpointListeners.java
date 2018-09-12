/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;

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
    @FunctionalInterface
    public interface GlobalCheckpointListener {
        /**
         * Callback when the global checkpoint is updated or the shard is closed. If the shard is closed, the value of the global checkpoint
         * will be set to {@link org.elasticsearch.index.seqno.SequenceNumbers#UNASSIGNED_SEQ_NO} and the exception will be non-null. If the
         * global checkpoint is updated, the exception will be null.
         *
         * @param globalCheckpoint the updated global checkpoint
         * @param e                if non-null, the shard is closed
         */
        void accept(long globalCheckpoint, IndexShardClosedException e);
    }

    // guarded by this
    private boolean closed;
    private volatile List<GlobalCheckpointListener> listeners;
    private long lastKnownGlobalCheckpoint = UNASSIGNED_SEQ_NO;

    private final ShardId shardId;
    private final Executor executor;
    private final Logger logger;

    /**
     * Construct a global checkpoint listeners collection.
     *
     * @param shardId  the shard ID on which global checkpoint updates can be listened to
     * @param executor the executor for listener notifications
     * @param logger   a shard-level logger
     */
    GlobalCheckpointListeners(
            final ShardId shardId,
            final Executor executor,
            final Logger logger) {
        this.shardId = Objects.requireNonNull(shardId);
        this.executor = Objects.requireNonNull(executor);
        this.logger = Objects.requireNonNull(logger);
    }

    /**
     * Add a global checkpoint listener. If the global checkpoint is above the current global checkpoint known to the listener then the
     * listener will be asynchronously notified on the executor used to construct this collection of global checkpoint listeners. If the
     * shard is closed then the listener will be asynchronously notified on the executor used to construct this collection of global
     * checkpoint listeners. The listener will only be notified of at most one event, either the global checkpoint is updated or the shard
     * is closed. A listener must re-register after one of these events to receive subsequent events.
     *
     * @param currentGlobalCheckpoint the current global checkpoint known to the listener
     * @param listener                the listener
     */
    synchronized void add(final long currentGlobalCheckpoint, final GlobalCheckpointListener listener) {
        if (closed) {
            executor.execute(() -> notifyListener(listener, UNASSIGNED_SEQ_NO, new IndexShardClosedException(shardId)));
            return;
        }
        if (lastKnownGlobalCheckpoint > currentGlobalCheckpoint) {
            // notify directly
            executor.execute(() -> notifyListener(listener, lastKnownGlobalCheckpoint, null));
        } else {
            if (listeners == null) {
                listeners = new ArrayList<>();
            }
            listeners.add(listener);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        notifyListeners(UNASSIGNED_SEQ_NO, new IndexShardClosedException(shardId));
    }

    synchronized int pendingListeners() {
        return listeners == null ? 0 : listeners.size();
    }

    /**
     * Invoke to notify all registered listeners of an updated global checkpoint.
     *
     * @param globalCheckpoint the updated global checkpoint
     */
    synchronized void globalCheckpointUpdated(final long globalCheckpoint) {
        assert globalCheckpoint >= NO_OPS_PERFORMED;
        assert globalCheckpoint > lastKnownGlobalCheckpoint
                : "updated global checkpoint [" + globalCheckpoint + "]"
                + " is not more than the last known global checkpoint [" + lastKnownGlobalCheckpoint + "]";
        lastKnownGlobalCheckpoint = globalCheckpoint;
        notifyListeners(globalCheckpoint, null);
    }

    private void notifyListeners(final long globalCheckpoint, final IndexShardClosedException e) {
        assert Thread.holdsLock(this);
        assert (globalCheckpoint == UNASSIGNED_SEQ_NO && e != null) || (globalCheckpoint >= NO_OPS_PERFORMED && e == null);
        if (listeners != null) {
            // capture the current listeners
            final List<GlobalCheckpointListener> currentListeners = listeners;
            listeners = null;
            if (currentListeners != null) {
                executor.execute(() -> {
                    for (final GlobalCheckpointListener listener : currentListeners) {
                        notifyListener(listener, globalCheckpoint, e);
                    }
                });
            }
        }
    }

    private void notifyListener(final GlobalCheckpointListener listener, final long globalCheckpoint, final IndexShardClosedException e) {
        try {
            listener.accept(globalCheckpoint, e);
        } catch (final Exception caught) {
            if (globalCheckpoint != UNASSIGNED_SEQ_NO) {
                logger.warn(
                        new ParameterizedMessage(
                                "error notifying global checkpoint listener of updated global checkpoint [{}]",
                                globalCheckpoint),
                        caught);
            } else {
                logger.warn("error notifying global checkpoint listener of closed shard", caught);
            }
        }
    }

}
