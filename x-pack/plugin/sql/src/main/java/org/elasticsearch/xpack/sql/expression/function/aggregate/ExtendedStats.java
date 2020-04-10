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

public class ExtendedStats extends CompoundNumericAggregate {

    public ExtendedStats(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<ExtendedStats> info() {
        return NodeInfo.create(this, ExtendedStats::new, field());
    }

    @Override
    public ExtendedStats replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() != 1) {
            throw new IllegalArgumentException("expected [1] child but received [" + newChildren.size() + "]");
        }
        return new ExtendedStats(source(), newChildren.get(0));
    }
}
