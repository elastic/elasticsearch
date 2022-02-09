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

public class VarSamp extends NumericAggregate implements ExtendedStatsEnclosed {
    public VarSamp(Source source, Expression field) {
        super(source, field);
    }

    @Override
    public String innerName() {
        return "variance_sampling";
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new VarSamp(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, VarSamp::new, field());
    }
}
