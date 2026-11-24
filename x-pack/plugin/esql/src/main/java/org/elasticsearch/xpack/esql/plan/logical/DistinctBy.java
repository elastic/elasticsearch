/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Keeps the first row of each {@code key}; with {@code failOnDuplicate} a second row fails the query instead. PromQL asserts
 * with it what Prometheus checks while it evaluates, that a result holds one series per label set at a step.
 */
public class DistinctBy extends UnaryPlan {
    private final Attribute key;
    private final boolean failOnDuplicate;

    public DistinctBy(Source source, LogicalPlan child, Attribute key, boolean failOnDuplicate) {
        super(source, child);
        this.key = key;
        this.failOnDuplicate = failOnDuplicate;
    }

    public Attribute key() {
        return key;
    }

    public boolean failOnDuplicate() {
        return failOnDuplicate;
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.of(key);
    }

    @Override
    public List<Attribute> output() {
        return child().output();
    }

    @Override
    public boolean expressionsResolved() {
        return key.resolved();
    }

    @Override
    public DistinctBy replaceChild(LogicalPlan newChild) {
        return new DistinctBy(source(), newChild, key, failOnDuplicate);
    }

    @Override
    protected NodeInfo<DistinctBy> info() {
        return NodeInfo.create(this, DistinctBy::new, child(), key, failOnDuplicate);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("DistinctBy is local only and not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("DistinctBy is local only and not serialized");
    }

    @Override
    public boolean skipTelemetry() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, failOnDuplicate, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DistinctBy other = (DistinctBy) obj;
        return Objects.equals(key, other.key) && failOnDuplicate == other.failOnDuplicate && Objects.equals(child(), other.child());
    }
}
