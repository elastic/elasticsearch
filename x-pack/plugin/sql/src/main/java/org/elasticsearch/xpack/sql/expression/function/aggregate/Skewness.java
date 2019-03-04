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
        if (newChildren.size() != 1) {
            throw new IllegalArgumentException("expected [1] child but received [" + newChildren.size() + "]");
        }
        return new Skewness(source(), newChildren.get(0));
    }

    @Override
    public String innerName() {
        return "skewness";
    }
}
