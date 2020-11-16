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

public class Percentiles extends CompoundNumericAggregate implements HasPercentileConfig {

    private final List<Expression> percents;
    private final Expression method;
    private final Expression methodParameter;

    public Percentiles(Source source, Expression field, List<Expression> percents, Expression method, Expression methodParameter) {
        super(source, field, percents);
        this.method = method;
        this.methodParameter = methodParameter;
        this.percents = percents;
    }

    @Override
    protected NodeInfo<Percentiles> info() {
        return NodeInfo.create(this, Percentiles::new, field(), percents, method, methodParameter);
    }

    @Override
    public Percentiles replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() < 2) {
            throw new IllegalArgumentException("expected at least [2] children but received [" + newChildren.size() + "]");
        }
        return new Percentiles(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()), method, methodParameter);
    }

    public List<Expression> percents() {
        return percents;
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

        Percentiles that = (Percentiles) o;

        return Objects.equals(method, that.method)
            && Objects.equals(methodParameter, that.methodParameter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), children(), method, methodParameter);
    }
}
