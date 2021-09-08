/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plan.logical;

import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

public class Distinct extends UnaryPlan {

    public Distinct(Source source, LogicalPlan child) {
        super(source, child);
    }

    @Override
    protected NodeInfo<Distinct> info() {
        return NodeInfo.create(this, Distinct::new, child());
    }

    @Override
    public Distinct replaceChild(LogicalPlan newChild) {
        return new Distinct(source(), newChild);
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }
}
