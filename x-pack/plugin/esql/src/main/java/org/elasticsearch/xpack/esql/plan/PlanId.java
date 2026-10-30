/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Identity of one {@link QueryPlan} node for in-process JSON plans (logs, EXPLAIN, PROFILE).
 * <p>
 * Values come from an {@link AtomicLong}, same idea as {@code NameId}: unique in one JVM, and stable
 * across repeated tests only in the sense that the counter starts over with the JVM. {@link #toString()}
 * is the decimal id used in JSON.
 * <p>
 * <b>Not part of plan equality.</b> Two plans with the same structure and different ids still compare equal.
 * <p>
 * <b>Not serialized.</b> {@code writeTo} does not write this id, and stream constructors mint a new one.
 * A plan rebuilt on another node therefore has different ids from the coordinator copy. Coordinator logs,
 * coordinator EXPLAIN rows, and coordinator PROFILE plans stay consistent because they use the in-memory
 * tree. A data node's {@code plan_json} still joins to that node's operator {@code plan_node_id}s, but
 * those ids do not match the coordinator's until a follow-up writes {@code PlanId} on the wire.
 */
public final class PlanId {
    private static final AtomicLong COUNTER = new AtomicLong();

    private final long id;

    public PlanId() {
        this.id = COUNTER.incrementAndGet();
    }

    /** Test hook. Production code must use {@link #PlanId()} so ids stay unique. */
    public PlanId(long id) {
        this.id = id;
    }

    public long id() {
        return id;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        return id == ((PlanId) obj).id;
    }

    @Override
    public String toString() {
        return Long.toString(id);
    }
}
