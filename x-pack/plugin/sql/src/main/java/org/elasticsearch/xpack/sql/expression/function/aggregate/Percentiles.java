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

public class Percentiles extends CompoundNumericAggregate {

    private final List<Expression> percents;

    public Percentiles(Source source, Expression field, List<Expression> percents) {
        super(source, field, percents);
        this.percents = percents;
    }

    @Override
    protected NodeInfo<Percentiles> info() {
        return NodeInfo.create(this, Percentiles::new, field(), percents);
    }

    @Override
    public Percentiles replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() < 2) {
            throw new IllegalArgumentException("expected more than one child but received [" + newChildren.size() + "]");
        }
        return new Percentiles(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }

    public List<Expression> percents() {
        return percents;
    }
}
