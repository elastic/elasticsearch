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

/**
 * Find the first value of the field ordered by the 2nd argument (if provided)
 */
public class First extends TopHits {

    public First(Source source, Expression field, Expression sortField) {
        super(source, field, sortField);
    }

    @Override
    protected NodeInfo<First> info() {
        return NodeInfo.create(this, First::new, field(), orderField());
    }

    @Override
    public First replaceChildren(List<Expression> newChildren) {
        return new First(source(), newChildren.get(0), newChildren.size() > 1 ? newChildren.get(1) : null);
    }
}
