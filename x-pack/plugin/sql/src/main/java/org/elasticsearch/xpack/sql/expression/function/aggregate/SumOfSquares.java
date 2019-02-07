/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.aggregate;

import java.util.List;
import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

public class SumOfSquares extends NumericAggregate implements ExtendedStatsEnclosed {

    public SumOfSquares(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<SumOfSquares> info() {
        return NodeInfo.create(this, SumOfSquares::new, field());
    }

    @Override
    public SumOfSquares replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 1) {
            throw new IllegalArgumentException("expected [1] child but received [" + newChildren.size() + "]");
        }
        return new SumOfSquares(source(), newChildren.get(0));
    }

    @Override
    public String innerName() {
        return "sum_of_squares";
    }
}
