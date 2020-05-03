/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.plan.logical;

import org.elasticsearch.xpack.ql.capabilities.Resolvables;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

/**
 * Filter that has one or multiple associated keys associated with.
 * Used inside Join or Sequence.
 */
public class KeyedFilter extends UnaryPlan {

    private final List<Expression> keys;

    public KeyedFilter(Source source, LogicalPlan child, List<Expression> keys) {
        super(source, child);
        this.keys = keys;
    }

    @Override
    protected NodeInfo<KeyedFilter> info() {
        return NodeInfo.create(this, KeyedFilter::new, child(), keys);
    }

    @Override
    protected KeyedFilter replaceChild(LogicalPlan newChild) {
        return new KeyedFilter(source(), newChild, keys);
    }
    
    public List<Expression> keys() {
        return keys;
    }

    @Override
    public boolean expressionsResolved() {
        return Resolvables.resolved(keys);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(keys, child());
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
                && Objects.equals(child(), other.child());
    }
}
