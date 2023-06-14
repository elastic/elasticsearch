/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

@Experimental
public class Median extends NumericAggregate {

    // TODO: Add the compression parameter
    public Median(Source source, Expression field) {
        super(source, field);
    }

    @Override
    protected NodeInfo<Median> info() {
        return NodeInfo.create(this, Median::new, field());
    }

    @Override
    public Median replaceChildren(List<Expression> newChildren) {
        return new Median(source(), newChildren.get(0));
    }
}
