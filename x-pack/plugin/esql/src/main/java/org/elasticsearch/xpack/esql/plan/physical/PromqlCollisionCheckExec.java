/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical counterpart of {@link org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCollisionCheck}. Passes rows
 * through unchanged but fails the query when two distinct source series collapse onto the same relabeled identity within
 * the same time bucket. Like {@link ChangePointExec}, it runs on the coordinator only and is therefore never serialized.
 */
public class PromqlCollisionCheckExec extends UnaryExec {

    private final List<Attribute> identity;
    private final Attribute bucket;

    public PromqlCollisionCheckExec(Source source, PhysicalPlan child, List<Attribute> identity, Attribute bucket) {
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
    protected NodeInfo<? extends PromqlCollisionCheckExec> info() {
        return NodeInfo.create(this, PromqlCollisionCheckExec::new, child(), identity, bucket);
    }

    @Override
    public PromqlCollisionCheckExec replaceChild(PhysicalPlan newChild) {
        return new PromqlCollisionCheckExec(source(), newChild, identity, bucket);
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
    public int hashCode() {
        return Objects.hash(super.hashCode(), identity, bucket);
    }

    @Override
    public boolean equals(Object other) {
        return super.equals(other)
            && Objects.equals(identity, ((PromqlCollisionCheckExec) other).identity)
            && Objects.equals(bucket, ((PromqlCollisionCheckExec) other).bucket);
    }
}
