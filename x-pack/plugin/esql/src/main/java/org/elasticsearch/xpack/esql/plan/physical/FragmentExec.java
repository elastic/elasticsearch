/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;
import java.util.Objects;

public class FragmentExec extends LeafExec implements EstimatesRowSize {

    private final LogicalPlan fragment;
    private final QueryBuilder esFilter;
    private final PhysicalPlan reducer; // datanode-level physical plan node that performs an intermediate (not partial) reduce

    /**
     * Estimate of the number of bytes that'll be loaded per position before
     * the stream of pages is consumed.
     */
    private final int estimatedRowSize;

    public FragmentExec(LogicalPlan fragment) {
        this(fragment.source(), fragment, null, 0, null);
    }

    public FragmentExec(Source source, LogicalPlan fragment, QueryBuilder esFilter, int estimatedRowSize, PhysicalPlan reducer) {
        super(source);
        this.fragment = fragment;
        this.esFilter = esFilter;
        this.estimatedRowSize = estimatedRowSize;
        this.reducer = reducer;
    }

    public LogicalPlan fragment() {
        return fragment;
    }

    public QueryBuilder esFilter() {
        return esFilter;
    }

    public Integer estimatedRowSize() {
        return estimatedRowSize;
    }

    public PhysicalPlan reducer() {
        return reducer;
    }

    @Override
    protected NodeInfo<FragmentExec> info() {
        return NodeInfo.create(this, FragmentExec::new, fragment, esFilter, estimatedRowSize, reducer);
    }

    @Override
    public List<Attribute> output() {
        return fragment.output();
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        int estimatedRowSize = state.consumeAllFields(false);
        return Objects.equals(estimatedRowSize, this.estimatedRowSize)
            ? this
            : new FragmentExec(source(), fragment, esFilter, estimatedRowSize, reducer);
    }

    public FragmentExec withFilter(QueryBuilder filter) {
        return Objects.equals(filter, this.esFilter) ? this : new FragmentExec(source(), fragment, filter, estimatedRowSize, reducer);
    }

    public FragmentExec withReducer(PhysicalPlan reducer) {
        return Objects.equals(reducer, this.reducer) ? this : new FragmentExec(source(), fragment, esFilter, estimatedRowSize, reducer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fragment, esFilter, estimatedRowSize, reducer);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        FragmentExec other = (FragmentExec) obj;
        return Objects.equals(fragment, other.fragment)
            && Objects.equals(esFilter, other.esFilter)
            && Objects.equals(estimatedRowSize, other.estimatedRowSize)
            && Objects.equals(reducer, other.reducer);
    }

    @Override
    public String nodeString() {
        StringBuilder sb = new StringBuilder();
        sb.append(nodeName());
        sb.append("[filter=");
        sb.append(esFilter);
        sb.append(", estimatedRowSize=");
        sb.append(estimatedRowSize);
        sb.append(", reducer=[");
        sb.append(reducer == null ? "" : reducer.toString());
        sb.append("], fragment=[<>\n");
        sb.append(fragment.toString());
        sb.append("<>]]");
        return sb.toString();
    }
}
