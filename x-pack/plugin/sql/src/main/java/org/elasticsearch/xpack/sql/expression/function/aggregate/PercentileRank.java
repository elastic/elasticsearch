/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

public class PercentileRank extends PercentileAggregate {

    public PercentileRank(Source source, Expression field, Expression value, Expression method, Expression methodParameter) {
        super(source, field, value, method, methodParameter);
    }

    @Override
    protected NodeInfo<PercentileRank> info() {
        return NodeInfo.create(this, PercentileRank::new, field(), value(), method(), methodParameter());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new PercentileRank(source(), newChildren.get(0), newChildren.get(1), method(), methodParameter());
    }

    public Expression value() {
        return parameter();
    }
}
