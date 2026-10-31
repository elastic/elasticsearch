/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSource;
import org.elasticsearch.compute.operator.lookup.HashJoinBridge;
import org.elasticsearch.core.Releasable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Connects stage source and sink plan nodes to the exchange handlers for their edges. */
public record ExchangeWiring(
    Map<String, Supplier<ExchangeSource>> sources,
    Map<String, List<ExchangeSinkHandler>> sinks,
    Map<String, SharedHashJoin> sharedHashJoins
) {
    public ExchangeWiring(Map<String, Supplier<ExchangeSource>> sources, Map<String, List<ExchangeSinkHandler>> sinks) {
        this(sources, sinks, Map.of());
    }

    public ExchangeWiring {
        sources = Map.copyOf(sources);
        sinks = sinks.entrySet().stream().collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> List.copyOf(e.getValue())));
        sharedHashJoins = Map.copyOf(sharedHashJoins);
    }

    /** Returns the source supplier for an edge or fails if the staged plan has no matching input. */
    public Supplier<ExchangeSource> source(String edgeKey) {
        Supplier<ExchangeSource> source = sources.get(edgeKey);
        if (source == null) {
            throw new IllegalStateException("bug: missing exchange source for edge [" + edgeKey + "]");
        }
        return source;
    }

    /** Returns the sink handlers for an edge or fails if the staged plan has no matching output. */
    public List<ExchangeSinkHandler> sinks(String edgeKey) {
        List<ExchangeSinkHandler> edgeSinks = sinks.get(edgeKey);
        if (edgeSinks == null) {
            throw new IllegalStateException("bug: missing exchange sinks for edge [" + edgeKey + "]");
        }
        return edgeSinks;
    }

    /**
     * Owns a broadcast build for one stage on one node. The stage holds this reference until all its lanes finish;
     * each build, probe, and payload-loading operator retains its own reference.
     */
    public static final class SharedHashJoin implements Releasable {
        private final HashJoinBridge bridge = new HashJoinBridge();
        private final AtomicBoolean buildClaimed = new AtomicBoolean();

        public HashJoinBridge bridge() {
            return bridge;
        }

        /** Elects exactly one lane to plan the build driver. */
        public boolean claimBuild() {
            return buildClaimed.compareAndSet(false, true);
        }

        @Override
        public void close() {
            bridge.decRef();
        }
    }
}
