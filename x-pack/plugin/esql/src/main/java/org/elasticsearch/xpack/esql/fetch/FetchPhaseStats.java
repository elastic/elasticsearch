/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.FetchPhaseOutcomes;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.fetch.FetchPhasePolicy.Outcome;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

/**
 * What the fetch phase did for one query, for the query log and the usage counters: the decision of the planner, and what
 * the fetches of the query sent and loaded. A profile has the same per operator, but production queries run without one.
 * <p>
 * Coordinator-only and never serialized. Every execution of the query adds to the same one, from the threads that receive
 * the fetch responses.
 */
public final class FetchPhaseStats {
    @Nullable
    private volatile FetchPhaseOutcomes outcomes;
    private final LongAdder documents = new LongAdder();
    private final LongAdder requests = new LongAdder();
    private final LongAdder tookNanos = new LongAdder();
    private final Set<String> nodes = ConcurrentCollections.newConcurrentSet();

    /**
     * The decisions of the planner for the plans of the query, which it records as it optimizes them.
     */
    public void plannedBy(FetchPhaseOutcomes outcomes) {
        this.outcomes = outcomes;
    }

    /**
     * What the planner did with the fetch phase for the query, as {@code EXPLAIN} shows it. {@code null} when the query left
     * the fetch phase alone: the build lacks the feature, or the cluster setting is off and the query sets no pragma.
     */
    @Nullable
    public Outcome outcome() {
        FetchPhaseOutcomes planned = outcomes;
        FetchPhaseOutcomes.Decision decision = planned == null ? null : planned.explained();
        return decision == null ? null : decision.outcome();
    }

    /**
     * A batch of a fetch ended: it asked each of {@code contacted} for some of its {@code documents}, one request per node,
     * and its last answer came {@code nanos} after it went out.
     */
    public void batchEnded(long documents, Collection<String> contacted, long nanos) {
        this.documents.add(documents);
        this.requests.add(contacted.size());
        this.tookNanos.add(nanos);
        this.nodes.addAll(contacted);
    }

    /**
     * The documents the fetches asked for. A document that several rows read counts once per batch.
     */
    public long documents() {
        return documents.sum();
    }

    public long requests() {
        return requests.sum();
    }

    /**
     * The distinct nodes the fetches asked, by name.
     */
    public int nodesContacted() {
        return nodes.size();
    }

    /**
     * How long the batches waited for their fetches, summed over the batches. Batches that wait at the same time each count.
     */
    public long tookNanos() {
        return tookNanos.sum();
    }
}
