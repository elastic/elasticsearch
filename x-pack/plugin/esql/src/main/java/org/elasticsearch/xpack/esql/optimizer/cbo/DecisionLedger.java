/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo;

import org.elasticsearch.core.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * What the planner did with each published decision of one optimizer run. Consumers such as the mapper record the decision
 * they read and the alternative they used, so a profile can tell an intentional override from a bug: the ledger is compared
 * with an independent observation of the physical plan. A consumer may read a decision more than once, and the latest record
 * wins. Mapping can run on another thread, so the ledger is thread-safe.
 */
public final class DecisionLedger {
    private final Map<Slot, Entry> entries = new ConcurrentHashMap<>();

    /**
     * Records how a consumer used a decision.
     *
     * @param decided the alternative the consumer read from {@link PlanDecisions}
     * @param effective the alternative the consumer used
     * @param overrideReason why the consumer did not use {@code decided}, or {@code null} if it did
     */
    public void record(DecisionKind kind, Object key, String decided, String effective, @Nullable String overrideReason) {
        entries.put(new Slot(kind.name(), key), new Entry(decided, effective, overrideReason));
    }

    /** The latest record for the decision, or {@code null} if no consumer read it. */
    @Nullable
    public Entry find(DecisionKind kind, Object key) {
        return entries.get(new Slot(kind.name(), key));
    }

    /**
     * One consumer record.
     *
     * @param decided the alternative the consumer read
     * @param effective the alternative the consumer used
     * @param overrideReason why {@code effective} differs from {@code decided}, or {@code null}
     */
    public record Entry(String decided, String effective, @Nullable String overrideReason) {
        public Entry {
            Objects.requireNonNull(decided, "decided");
            Objects.requireNonNull(effective, "effective");
        }
    }

    private record Slot(String kind, Object key) {}
}
