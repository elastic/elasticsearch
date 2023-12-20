/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.plan.logical;

import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.List;

public class Sample extends AbstractJoin {

    public Sample(Source source, List<KeyedFilter> queries) {
        super(source, queries);
    }

    @Override
    protected NodeInfo<? extends Sample> info() {
        return NodeInfo.create(this, Sample::new, queries);
    }

    @Override
    public Sample replaceChildren(List<LogicalPlan> newChildren) {
        return new Sample(source(), asKeyed(newChildren));
    }

    public Sample with(List<KeyedFilter> queries) {
        return new Sample(source(), queries);
    }
}
