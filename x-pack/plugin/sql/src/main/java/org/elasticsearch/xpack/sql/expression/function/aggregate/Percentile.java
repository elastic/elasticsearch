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

public class Percentile extends PercentileAggregate {

    public Percentile(Source source, Expression field, Expression percent, Expression method, Expression methodParameter) {
        super(source, field, percent, method, methodParameter);
    }

    @Override
    protected NodeInfo<Percentile> info() {
        return NodeInfo.create(this, Percentile::new, field(), percent(), method(), methodParameter());
    }

    @Override
    public Percentile replaceChildren(List<Expression> newChildren) {
        return new Percentile(source(), newChildren.get(0), newChildren.get(1), method(), methodParameter());
    }

    public Expression percent() {
        return parameter();
    }
}
