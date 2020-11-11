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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.ql.expression.function.Functions.countOfNonNullOptionalArgs;
import static org.elasticsearch.xpack.sql.expression.function.aggregate.PercentileMethodConfiguration.defaultMethod;
import static org.elasticsearch.xpack.sql.expression.function.aggregate.PercentileMethodConfiguration.defaultMethodParameter;

public class Percentiles extends CompoundNumericAggregate {

    private final List<Expression> percents;
    private final Expression method;
    private final Expression methodParameter;

    public Percentiles(Source source, Expression field, Expression method, Expression methodParameter, List<Expression> percents) {
        super(source, field, Stream.concat(
            Stream.of((method = defaultMethod(source, method)),
                (methodParameter = defaultMethodParameter(methodParameter))).filter(Objects::nonNull),
            percents.stream()
        ).collect(Collectors.toList()));
        this.method = method;
        this.methodParameter = methodParameter;
        this.percents = percents;
    }

    @Override
    protected NodeInfo<Percentiles> info() {
        return NodeInfo.create(this, Percentiles::new, field(), method, methodParameter, percents);
    }

    @Override
    public Percentiles replaceChildren(List<Expression> newChildren) {
        if (children().size() != newChildren.size()) {
            throw new IllegalArgumentException("expected [" + children().size() + "] children but received [" + newChildren.size() + "]");
        }
        return new Percentiles(source(), newChildren.get(0),
            method == null ? null : newChildren.get(1),
            methodParameter == null ? null : newChildren.get(1 + countOfNonNullOptionalArgs(method)),
            newChildren.subList(1 + countOfNonNullOptionalArgs(method, methodParameter), newChildren.size())
        );
    }

    public List<Expression> percents() {
        return percents;
    }

    public Expression method() {
        return method;
    }

    public Expression methodParameter() {
        return methodParameter;
    }
}
