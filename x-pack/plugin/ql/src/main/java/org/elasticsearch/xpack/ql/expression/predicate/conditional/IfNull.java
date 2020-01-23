/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.expression.predicate.conditional;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Arrays;
import java.util.List;

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
        return NodeInfo.create(this, IfNull::new, children().get(0), children().get(1));
    }
}
