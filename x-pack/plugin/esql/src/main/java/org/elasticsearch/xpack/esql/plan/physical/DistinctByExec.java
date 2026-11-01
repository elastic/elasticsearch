/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.Objects;

/**
 * Physical plan for {@link org.elasticsearch.compute.operator.DistinctByOperator}.
 */
public class DistinctByExec extends UnaryExec {

    private final Attribute key;
    private final boolean failOnDuplicate;

    public DistinctByExec(Source source, PhysicalPlan child, Attribute key, boolean failOnDuplicate) {
        super(source, child);
        this.key = key;
        this.failOnDuplicate = failOnDuplicate;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    public Attribute key() {
        return key;
    }

    public boolean failOnDuplicate() {
        return failOnDuplicate;
    }

    @Override
    public DistinctByExec replaceChild(PhysicalPlan newChild) {
        return new DistinctByExec(source(), newChild, key, failOnDuplicate);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, DistinctByExec::new, child(), key, failOnDuplicate);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        DistinctByExec that = (DistinctByExec) o;
        return failOnDuplicate == that.failOnDuplicate && key.equals(that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), key, failOnDuplicate);
    }
}
