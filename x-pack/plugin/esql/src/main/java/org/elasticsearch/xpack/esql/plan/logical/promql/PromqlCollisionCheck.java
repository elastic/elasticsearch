/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.capabilities.Resolvables;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Detects PromQL relabel collisions produced by {@code label_replace}/{@code label_join}: two distinct source series
 * mapped onto the same label set at the same time bucket. It is inserted directly above the relabel's first-pass
 * identity seam - before any consuming outer aggregate and before the final time-series collapse - so a per-series,
 * per-bucket row is still visible; an outer {@code by(dst)} aggregate would otherwise merge the colliding rows first and
 * hide the collision.
 * <p>
 * The check passes non-colliding rows through unchanged, but fails the query on the first collision (mirroring PromQL's
 * "vector cannot contain metrics with the same labelset" evaluation error), so an ambiguous relabel never yields a
 * silently merged result. It must run where the whole vector is visible, so it executes on the coordinator only. The
 * node is an internal translation artifact that never leaves the coordinator, hence it is not serialized.
 * <p>
 * The {@code identity} columns make up the (possibly rewritten) series identity: the {@code _timeseries} blob for
 * whole-identity output, or the tuple of grouping columns for columns-only output. The {@code bucket} column is the time
 * bucket, so identities that coincide only at different instants are not flagged.
 */
public class PromqlCollisionCheck extends UnaryPlan implements ExecutesOn.Coordinator {

    private final List<Attribute> identity;
    private final Attribute bucket;

    public PromqlCollisionCheck(Source source, LogicalPlan child, List<Attribute> identity, Attribute bucket) {
        super(source, child);
        this.identity = identity;
        this.bucket = bucket;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    protected NodeInfo<PromqlCollisionCheck> info() {
        return NodeInfo.create(this, PromqlCollisionCheck::new, child(), identity, bucket);
    }

    @Override
    public PromqlCollisionCheck replaceChild(LogicalPlan newChild) {
        return new PromqlCollisionCheck(source(), newChild, identity, bucket);
    }

    public List<Attribute> identity() {
        return identity;
    }

    public Attribute bucket() {
        return bucket;
    }

    @Override
    protected AttributeSet computeReferences() {
        return Expressions.references(identity).combine(bucket.references());
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(identity) && bucket.resolved();
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), identity, bucket);
    }

    @Override
    public boolean equals(Object other) {
        return super.equals(other)
            && Objects.equals(identity, ((PromqlCollisionCheck) other).identity)
            && Objects.equals(bucket, ((PromqlCollisionCheck) other).bucket);
    }
}
