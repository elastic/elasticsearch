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

public class StddevPop extends NumericAggregate implements ExtendedStatsEnclosed {

    public StddevPop(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<StddevPop> info() {
        return NodeInfo.create(this, StddevPop::new, field());
    }

    @Override
    public StddevPop replaceChildren(List<Expression> newChildren) {
        return new StddevPop(source(), newChildren.get(0));
    }

    @Override
    public String innerName() {
        return "std_deviation";
    }
}
