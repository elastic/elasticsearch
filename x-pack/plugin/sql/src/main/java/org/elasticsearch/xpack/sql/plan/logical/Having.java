/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.plan.logical;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

/**
 * Dedicated class for SQL HAVING.
 * It doesn't add any functionality and itself, acts as a marker.
 */
public class Having extends Filter {

    public Having(Source source, LogicalPlan child, Expression condition) {
        super(source, child, condition);
    }

    @Override
    protected NodeInfo<Filter> info() {
        return NodeInfo.create(this, Having::new, child(), condition());
    }

    @Override
    public Having replaceChild(LogicalPlan newChild) {
        return new Having(source(), newChild, condition());
    }

    @Override
    public Filter with(Expression condition) {
        return new Having(source(), child(), condition);
    }

    @Override
    public Filter with(LogicalPlan child, Expression condition) {
        return new Having(source(), child, condition);
    }
}
