/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;
import java.util.Objects;

public class PercentileRanks extends CompoundNumericAggregate implements HasPercentileConfig {

    private final List<Expression> values;
    private final Expression method;
    private final Expression methodParameter;

    public PercentileRanks(Source source, Expression field, List<Expression> values, Expression method, Expression methodParameter) {
        super(source, field, values);
        this.method = method;
        this.methodParameter = methodParameter;
        this.values = values;
    }

    @Override
    protected NodeInfo<PercentileRanks> info() {
        return NodeInfo.create(this, PercentileRanks::new, field(), values, method, methodParameter);
    }

    @Override
    public PercentileRanks replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() < 2) {
            throw new IllegalArgumentException("expected at least [2] children but received [" + newChildren.size() + "]");
        }
        return new PercentileRanks(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()), method, methodParameter);
    }

    public List<Expression> values() {
        return values;
    }

    @Override
    public Expression method() {
        return method;
    }

    @Override
    public Expression methodParameter() {
        return methodParameter;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        PercentileRanks that = (PercentileRanks) o;

        return Objects.equals(method, that.method)
            && Objects.equals(methodParameter, that.methodParameter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), children(), method, methodParameter);
    }

}
