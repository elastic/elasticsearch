/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.operator.exchange.ExchangeSinkHandler;
import org.elasticsearch.compute.operator.exchange.ExchangeSource;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Connects stage source and sink plan nodes to the exchange handlers for their edges. */
public record ExchangeWiring(Map<String, Supplier<ExchangeSource>> sources, Map<String, List<ExchangeSinkHandler>> sinks) {
    public ExchangeWiring {
        sources = Map.copyOf(sources);
        sinks = sinks.entrySet().stream().collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> List.copyOf(e.getValue())));
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
}
