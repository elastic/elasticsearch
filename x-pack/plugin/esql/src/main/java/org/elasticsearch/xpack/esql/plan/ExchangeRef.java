/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.PlanProfile;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Names one exchange, the hop where pages move from the plan fragment that produces them to the one that consumes them,
 * so a plan viewer can join the two sides. Every {@code ExchangeSinkExec}, {@code ExchangeSourceExec} and
 * {@code ExchangeExec} of a hop carries the same {@link #id}, and so do the sink and source operators that run them.
 * <p>
 * The id names the hop, not a pair of nodes. A {@code FORK} or {@code UNION} merge has one source and many sinks, one for each
 * branch, and they all share it. {@link #childIndex} says which branch a sink is, by its position among the merge's children.
 * <p>
 * Unlike {@link PlanId} this is written on the wire, because the two sides of a hop are planned on different nodes. The ids are
 * minted from a counter and a per-JVM tag, so two ids from different nodes of one query cannot collide. They are only meant to
 * be compared inside one response, and are not stable across runs.
 *
 * @param id         the shared name of the hop
 * @param childIndex position of this sink among the children of a merge, or {@code null} when the hop has no branches
 */
public record ExchangeRef(String id, @Nullable Integer childIndex) {

    private static final String JVM_TAG = Integer.toHexString(Randomness.get().nextInt(0x1000000));
    private static final AtomicLong COUNTER = new AtomicLong();

    public ExchangeRef {
        if (id == null) {
            throw new IllegalArgumentException("exchange id must not be null");
        }
    }

    /** A fresh id for a new hop. */
    public static String newId() {
        return "ex-" + JVM_TAG + "-" + COUNTER.incrementAndGet();
    }

    public static ExchangeRef newExchange() {
        return new ExchangeRef(newId(), null);
    }

    public ExchangeRef withChildIndex(int index) {
        return new ExchangeRef(id, index);
    }

    /** Writes {@code ref}, or nothing when the peer is too old to read it, which is the same as no ref on the node. */
    public static void writeOptionalTo(StreamOutput out, @Nullable ExchangeRef ref) throws IOException {
        if (out.getTransportVersion().supports(PlanProfile.JSON_PLANS_VERSION)) {
            out.writeBoolean(ref != null);
            if (ref != null) {
                out.writeString(ref.id);
                out.writeOptionalVInt(ref.childIndex);
            }
        }
    }

    @Nullable
    public static ExchangeRef readOptionalFrom(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(PlanProfile.JSON_PLANS_VERSION) && in.readBoolean()) {
            return new ExchangeRef(in.readString(), in.readOptionalVInt());
        }
        return null;
    }
}
