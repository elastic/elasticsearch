/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.plan.logical;

import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

public class Join extends UnaryPlan {

    public Join(Source source, LogicalPlan child, List<? extends NamedExpression> projections) {
        super(source, child);
        this.projections = projections;
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return null;
    }


    @Override
    protected UnaryPlan replaceChild(LogicalPlan newChild) {
        return null;
    }

    @Override
    public boolean expressionsResolved() {
        return false;
    }
}
