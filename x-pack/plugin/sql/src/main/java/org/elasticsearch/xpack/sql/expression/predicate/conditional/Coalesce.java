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

import java.util.List;

import static org.elasticsearch.xpack.sql.expression.predicate.conditional.ConditionalProcessor.ConditionalOperation.COALESCE;

public class Coalesce extends ArbitraryConditionalFunction {

    public Coalesce(Source source, List<Expression> fields) {
        super(source, fields, COALESCE);
    }

    @Override
    protected NodeInfo<? extends Coalesce> info() {
        return NodeInfo.create(this, Coalesce::new, children());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new Coalesce(source(), newChildren);
    }

    @Override
    public boolean foldable() {
        // if the first entry is foldable, so is coalesce
        // that's because the nulls are eliminated by the optimizer
        // and if the first expression is folded (and not null), the rest do not matter
        List<Expression> children = children();
        return (children.isEmpty() || (children.get(0).foldable() && children.get(0).fold() != null));
    }

    @Override
    public Object fold() {
        List<Expression> children = children();
        return children.isEmpty() ? null : children.get(0).fold();
    }
}
