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

/**
 * Find the last value of the field ordered by the 2nd argument (if provided)
 */
public class Last extends TopHits {

    public Last(Source source, Expression field, Expression sortField) {
        super(source, field, sortField);
    }

    @Override
    protected NodeInfo<Last> info() {
        return NodeInfo.create(this, Last::new, field(), orderField());
    }

    @Override
    public Last replaceChildren(List<Expression> newChildren) {
        if (newChildren.size() > 2) {
            throw new IllegalArgumentException("expected one or two children but received [" + newChildren.size() + "]");
        }
        return new Last(source(), newChildren.get(0), newChildren.size() > 1 ? newChildren.get(1) : null);
    }
}
