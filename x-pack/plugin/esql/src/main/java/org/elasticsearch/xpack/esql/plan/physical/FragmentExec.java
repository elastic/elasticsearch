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
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
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

    /**
     * Coordinator-only flag: {@code true} when this fragment originates from a view branch inside a {@code ViewUnionAll}.
     * View-branch fragments must NOT receive the raw DSL {@code request.filter()} as a Lucene query (via
     * {@code PlannerUtils.integrateEsFilterIntoFragment}), because the filter has already been applied as a logical
     * {@code Filter} above the view's output boundary. Pushing it into the Lucene scan would apply it before any
     * aggregation or field computation the view performs, producing wrong results for computed fields.
     *
     * <p>This flag is intentionally <em>not serialised</em>. It is set on the coordinator before the filter is
     * integrated and cleared (reads as {@code false}) on any node that deserialises the plan — by that point the
     * correct filter has already been stamped (or deliberately omitted) on the fragment's {@code esFilter} field.
     */
    private final boolean fromViewBranch;

    public FragmentExec(LogicalPlan fragment) {
        this(fragment.source(), fragment, null, 0);
    }

    public FragmentExec(Source source, LogicalPlan fragment, QueryBuilder esFilter, int estimatedRowSize) {
        this(source, fragment, esFilter, estimatedRowSize, false);
    }

    private FragmentExec(Source source, LogicalPlan fragment, QueryBuilder esFilter, int estimatedRowSize, boolean fromViewBranch) {
        super(source);
        this.fragment = fragment;
        this.esFilter = esFilter;
        this.estimatedRowSize = estimatedRowSize;
        this.fromViewBranch = fromViewBranch;
    }

    private FragmentExec(StreamInput in) throws IOException {
        super(Source.readFrom((PlanStreamInput) in));
        this.fragment = in.readNamedWriteable(LogicalPlan.class);
        this.esFilter = in.readOptionalNamedWriteable(QueryBuilder.class);
        this.estimatedRowSize = in.readVInt();
        this.fromViewBranch = false; // coordinator-only; not serialised
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

    /** Returns whether this fragment is a view-branch scan that must not receive the raw Lucene esFilter. */
    public boolean isFromViewBranch() {
        return fromViewBranch;
    }

    /** Returns a copy of this fragment marked as originating from a view branch (see {@link #fromViewBranch}). */
    public FragmentExec asFromViewBranch() {
        return fromViewBranch ? this : new FragmentExec(source(), fragment, esFilter, estimatedRowSize, true);
    }

    @Override
    public PhysicalPlan estimateRowSize(State state) {
        int estimatedRowSize = state.consumeAllFields(false);
        return Objects.equals(estimatedRowSize, this.estimatedRowSize)
            ? this
            : new FragmentExec(source(), fragment, esFilter, estimatedRowSize, fromViewBranch);
    }

    public FragmentExec withFragment(LogicalPlan fragment) {
        return Objects.equals(fragment, this.fragment)
            ? this
            : new FragmentExec(source(), fragment, esFilter, estimatedRowSize, fromViewBranch);
    }

    public FragmentExec withFilter(QueryBuilder filter) {
        return Objects.equals(filter, this.esFilter)
            ? this
            : new FragmentExec(source(), fragment, filter, estimatedRowSize, fromViewBranch);
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
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        sb.append(nodeName());
        // esFilter is a raw QueryBuilder DSL from request.filter() — opaque content; route it through
        // the opaque mapper so it prints raw under identity and redacts under anonymization.
        sb.append("[filter=").append(mapper.opaque(String.valueOf(esFilter)));
        sb.append(", estimatedRowSize=").append(estimatedRowSize);
        sb.append(", reducer=[], fragment=[<>\n");
        sb.append(fragment.toString(format, mapper));
        sb.append("<>]]");
    }

}
