/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical;

import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

public class Distinct extends UnaryPlan {

    public Distinct(Source source, LogicalPlan child) {
        super(source, child);
    }

    @Override
    protected NodeInfo<Distinct> info() {
        return NodeInfo.create(this, Distinct::new, child());
    }

    @Override
    protected Distinct replaceChild(LogicalPlan newChild) {
        return new Distinct(source(), newChild);
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }
}
