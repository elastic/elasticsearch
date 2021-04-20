/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

public class StartsWith extends org.elasticsearch.xpack.ql.expression.function.scalar.string.StartsWith {

    public StartsWith(Source source, Expression input, Expression pattern, boolean caseInsensitive) {
        super(source, input, pattern, caseInsensitive);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StartsWith::new, input(), pattern(), isCaseInsensitive());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StartsWith(source(), newChildren.get(0), newChildren.get(1), isCaseInsensitive());
    }
}
