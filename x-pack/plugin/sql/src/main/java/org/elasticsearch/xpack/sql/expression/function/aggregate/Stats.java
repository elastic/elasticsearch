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

public class Stats extends CompoundNumericAggregate {

    public Stats(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Stats> info() {
        return NodeInfo.create(this, Stats::new, field());
    }

    @Override
    public Stats replaceChildren(List<Expression> newChildren) {
        return new Stats(source(), newChildren.get(0));
    }

    public static boolean isTypeCompatible(Expression e) {
        return e instanceof Min || e instanceof Max || e instanceof Avg || e instanceof Sum;
    }
}
