/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.plan.logical;

import org.elasticsearch.xpack.ql.capabilities.Resolvables;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Filter that has one or multiple associated keys associated with.
 * Used inside Join or Sequence.
 */
public class KeyedFilter extends UnaryPlan {

    private final List<? extends NamedExpression> keys;
    private final Attribute timestamp;
    private final Attribute tiebreaker;

    public KeyedFilter(Source source, LogicalPlan child, List<? extends NamedExpression> keys, Attribute timestamp, Attribute tiebreaker) {
        super(source, child);
        this.keys = keys;
        this.timestamp = timestamp;
        this.tiebreaker = tiebreaker;
    }

    @Override
    protected NodeInfo<KeyedFilter> info() {
        return NodeInfo.create(this, KeyedFilter::new, child(), keys, timestamp, tiebreaker);
    }

    @Override
    public KeyedFilter replaceChild(LogicalPlan newChild) {
        return new KeyedFilter(source(), newChild, keys, timestamp, tiebreaker);
    }

    public List<? extends NamedExpression> keys() {
        return keys;
    }

    public Attribute timestamp() {
        return timestamp;
    }

    public Attribute tiebreaker() {
        return tiebreaker;
    }

    public List<? extends NamedExpression> extractionAttributes() {
        List<NamedExpression> out = new ArrayList<>();

        out.add(timestamp);
        if (Expressions.isPresent(tiebreaker)) {
            out.add(tiebreaker);
        }

        out.addAll(keys);
        return out;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(keys) && timestamp.resolved() && tiebreaker.resolved();
    }

    @Override
    public int hashCode() {
        return Objects.hash(keys, timestamp, tiebreaker, child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        KeyedFilter other = (KeyedFilter) obj;

        return Objects.equals(keys, other.keys)
            && Objects.equals(timestamp, other.timestamp)
            && Objects.equals(tiebreaker, other.tiebreaker)
            && Objects.equals(child(), other.child());
    }
}
