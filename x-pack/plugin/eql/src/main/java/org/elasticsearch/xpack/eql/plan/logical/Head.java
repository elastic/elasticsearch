/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.plan.logical;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.plan.logical.Limit;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

public class Head extends LimitWithOffset {

    public Head(Source source, Expression limit, LogicalPlan child) {
        super(source, limit, child);
    }

    @Override
    protected NodeInfo<Limit> info() {
        return NodeInfo.create(this, Head::new, limit(), child());
    }

    @Override
    protected Head replaceChild(LogicalPlan newChild) {
        return new Head(source(), limit(), newChild);
    }
}