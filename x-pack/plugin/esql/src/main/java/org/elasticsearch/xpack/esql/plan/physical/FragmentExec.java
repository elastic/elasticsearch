/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class FragmentExec extends LeafExec implements EstimatesRowSize {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "FragmentExec",
        FragmentExec::new
    );

    private final LogicalPlan fragment;
    private final QueryBuilder esFilter;

    /**
     * Estimate of the number of bytes that'll be loaded per position before
     * the stream of pages is consumed.
     */
    private final int estimatedRowSize;

    public FragmentExec(LogicalPlan fragment) {
        this(fragment.source(), fragment, null, 0);
    }

    public FragmentExec(Source source, LogicalPlan fragment, QueryBuilder esFilter, int estimatedRowSize) {
        super(source);
        this.fragment = fragment;
        this.esFilter = esFilter;
        this.estimatedRowSize = estimatedRowSize;
    }

    private FragmentExec(StreamInput in) throws IOException {
        super(Source.readFrom((PlanStreamInput) in));
        this.fragment = in.readNamedWriteable(LogicalPlan.class);
        this.esFilter = in.readOptionalNamedWriteable(QueryBuilder.class);
        this.estimatedRowSize = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(fragment());
        out.writeOptionalNamedWriteable(esFilter());
        out.writeVInt(estimatedRowSize);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
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

    @Override
    protected NodeInfo<FragmentExec> info() {
        return NodeInfo.create(this, FragmentExec::new, fragment, esFilter, estimatedRowSize);
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
            : new FragmentExec(source(), fragment, esFilter, estimatedRowSize);
    }

    public FragmentExec withFragment(LogicalPlan fragment) {
        return Objects.equals(fragment, this.fragment) ? this : new FragmentExec(source(), fragment, esFilter, estimatedRowSize);
    }

    public FragmentExec withFilter(QueryBuilder filter) {
        return Objects.equals(filter, this.esFilter) ? this : new FragmentExec(source(), fragment, filter, estimatedRowSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fragment, esFilter, estimatedRowSize);
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
            && Objects.equals(estimatedRowSize, other.estimatedRowSize);
    }

    @Override
    public String nodeString(NodeStringFormat format) {
        StringBuilder sb = new StringBuilder();
        sb.append(nodeName());
        sb.append("[filter=");
        sb.append(esFilter);
        sb.append(", estimatedRowSize=");
        sb.append(estimatedRowSize);
        sb.append(", reducer=[");
        sb.append("], fragment=[<>\n");
        sb.append(fragment.toString(format));
        sb.append("<>]]");
        return sb.toString();
    }
}
