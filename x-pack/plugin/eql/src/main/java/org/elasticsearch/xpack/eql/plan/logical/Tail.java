/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.plan.logical;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

public class Tail extends LimitWithOffset {

    public Tail(Source source, Expression limit, LogicalPlan child) {
        this(source, child, new Neg(limit.source(), limit));
    }

    /**
     * Constructor that does not negate the limit expression.
     */
    private Tail(Source source, LogicalPlan child, Expression limit) {
        super(source, limit, child);
    }

    @Override
    protected NodeInfo<Limit> info() {
        return NodeInfo.create(this, Tail::new, child(), limit());
    }

    @Override
    protected Tail replaceChild(LogicalPlan newChild) {
        return new Tail(source(), newChild, limit());
    }
}
