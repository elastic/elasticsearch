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
import java.util.Objects;

public class Sample extends AbstractJoin {

    private final int maxSamplesPerKey;

    public Sample(Source source, List<KeyedFilter> queries, int maxSamplesPerKey) {
        super(source, queries);
        this.maxSamplesPerKey = maxSamplesPerKey;
    }

    @Override
    protected NodeInfo<? extends Sample> info() {
        return NodeInfo.create(this, Sample::new, queries, maxSamplesPerKey);
    }

    @Override
    public Sample replaceChildren(List<LogicalPlan> newChildren) {
        return new Sample(source(), asKeyed(newChildren), maxSamplesPerKey);
    }

    public Sample with(List<KeyedFilter> queries) {
        return new Sample(source(), queries, maxSamplesPerKey);
    }

    public int maxSamplesPerKey() {
        return maxSamplesPerKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        Sample sample = (Sample) o;
        return maxSamplesPerKey == sample.maxSamplesPerKey;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), maxSamplesPerKey);
    }
}
