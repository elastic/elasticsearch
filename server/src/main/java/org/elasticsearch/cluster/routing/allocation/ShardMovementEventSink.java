/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Receives {@link ShardMovementEvent}s. Plugins register sinks via
 * {@link org.elasticsearch.plugins.ClusterPlugin#getShardMovementEventSinks()}.
 * <p>
 * {@link #accept} is called on the master service thread while a new routing table is being computed, so implementations must be
 * non-blocking and cheap: hand the event to a bounded queue and do any I/O elsewhere. Events that cannot be queued should be dropped
 * and counted rather than applying back-pressure to allocation. Implementations must not throw.
 */
public interface ShardMovementEventSink {

    ShardMovementEventSink NOOP = event -> {};

    void accept(ShardMovementEvent event);

    /**
     * Combines sinks so each receives every event. A sink that throws is skipped for that event and the failure is logged, so one
     * misbehaving sink cannot affect allocation or the other sinks.
     */
    static ShardMovementEventSink composite(Collection<? extends ShardMovementEventSink> sinks) {
        return switch (sinks.size()) {
            case 0 -> NOOP;
            case 1 -> new SafeSink(sinks.iterator().next());
            default -> {
                final var safeSinks = sinks.stream().map(SafeSink::new).toList();
                yield event -> {
                    for (final var sink : safeSinks) {
                        sink.accept(event);
                    }
                };
            }
        };
    }

    /**
     * Wraps a sink so that exceptions are logged (at WARN the first time, DEBUG thereafter) rather than propagated.
     */
    final class SafeSink implements ShardMovementEventSink {
        private static final Logger logger = LogManager.getLogger(SafeSink.class);

        private final ShardMovementEventSink delegate;
        private final AtomicBoolean warned = new AtomicBoolean();

        SafeSink(ShardMovementEventSink delegate) {
            this.delegate = delegate;
        }

        @Override
        public void accept(ShardMovementEvent event) {
            try {
                delegate.accept(event);
            } catch (Exception e) {
                if (warned.compareAndSet(false, true)) {
                    logger.warn(() -> "shard movement event sink [" + delegate + "] failed, further failures will be logged at DEBUG", e);
                } else {
                    logger.debug(() -> "shard movement event sink [" + delegate + "] failed", e);
                }
            }
        }
    }
}
