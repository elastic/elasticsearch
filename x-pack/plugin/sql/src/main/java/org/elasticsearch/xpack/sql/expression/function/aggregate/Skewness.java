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

public class Skewness extends NumericAggregate implements MatrixStatsEnclosed {

    public Skewness(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Skewness> info() {
        return NodeInfo.create(this, Skewness::new, field());
    }

    @Override
    public Skewness replaceChildren(List<Expression> newChildren) {
        return new Skewness(source(), newChildren.get(0));
    }

    @Override
    public String innerName() {
        return "skewness";
    }
}
