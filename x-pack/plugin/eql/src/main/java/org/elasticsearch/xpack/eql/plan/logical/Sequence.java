/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.plan.logical;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

import static java.util.Collections.singletonList;

public class Sequence extends Join {

    private final TimeValue maxSpan;

    public Sequence(Source source, List<KeyedFilter> queries, KeyedFilter until, TimeValue maxSpan, Attribute timestampField) {
        super(source, queries, until, timestampField);
        this.maxSpan = maxSpan;
    }

    private Sequence(Source source, List<LogicalPlan> queries, LogicalPlan until, TimeValue maxSpan, Attribute timestampField) {
        super(source, asKeyed(queries), asKeyed(until), timestampField);
        this.maxSpan = maxSpan;
    }

    @Override
    protected NodeInfo<Sequence> info() {
        return NodeInfo.create(this, Sequence::new, queries(), until(), maxSpan, timestampField());
    }

    @Override
    public Join replaceChildren(List<LogicalPlan> newChildren) {
        if (newChildren.size() < 2) {
            throw new EqlIllegalArgumentException("expected at least [2] children but received [{}]", newChildren.size());
        }
        int lastIndex = newChildren.size() - 1;
        return new Sequence(source(), newChildren.subList(0, lastIndex), newChildren.get(lastIndex), maxSpan, timestampField());
    }

    public TimeValue maxSpan() {
        return maxSpan;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxSpan, timestampField(), queries(), until());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Sequence other = (Sequence) obj;

        return Objects.equals(maxSpan, other.maxSpan)
                && Objects.equals(queries(), other.queries())
                && Objects.equals(until(), other.until())
                && Objects.equals(timestampField(), other.timestampField());
    }

    @Override
    public List<Object> nodeProperties() {
        return singletonList(maxSpan);
    }
}
