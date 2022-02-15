/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.ql.expression.Literal.NULL;

/**
 * Variant of {@link Coalesce} with two args used by MySQL and ODBC.
 */
public class IfNull extends Coalesce {

    public IfNull(Source source, Expression first, Expression second) {
        this(source, Arrays.asList(first, second));
    }

    private IfNull(Source source, List<Expression> expressions) {
        super(source, expressions);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new IfNull(source(), newChildren);
    }

    @Override
    protected NodeInfo<IfNull> info() {
        List<Expression> children = children();
        Expression first = children.size() > 0 ? children.get(0) : NULL;
        Expression second = children.size() > 0 ? children.get(1) : NULL;
        return NodeInfo.create(this, IfNull::new, first, second);
    }
}
