/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.TransportVersion;
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

    /**
     * Transport version added when {@link #singlePassAgg} was introduced. Older data nodes always
     * read {@code singlePassAgg = false} and run the normal two-phase aggregation path.
     */
    private static final TransportVersion SINGLE_PASS_AGG_TV = TransportVersion.fromName("esql_single_pass_agg");

    private final LogicalPlan fragment;
    private final QueryBuilder esFilter;

    /**
     * Estimate of the number of bytes that'll be loaded per position before
     * the stream of pages is consumed.
     */
    private final int estimatedRowSize;

    /**
     * When {@code true}, the data node should map the top-level aggregation to
     * {@link org.elasticsearch.compute.aggregation.AggregatorMode#SINGLE} and run a single driver
     * instead of splitting the work across many parallel instances. The coordinator receives the
     * final aggregation output directly, with no intermediate merging step.
     * <p>
     * Set by {@code CollapseSingleShardAggregate} when the query touches exactly one shard and the
     * {@code esql.single_shard_single_pass_aggregation} cluster setting is enabled.
     */
    private final boolean singlePassAgg;

    public FragmentExec(LogicalPlan fragment) {
        this(fragment.source(), fragment, null, 0, false);
    }

    public FragmentExec(Source source, LogicalPlan fragment, QueryBuilder esFilter, int estimatedRowSize) {
        this(source, fragment, esFilter, estimatedRowSize, false);
    }

    public FragmentExec(Source source, LogicalPlan fragment, QueryBuilder esFilter, int estimatedRowSize, boolean singlePassAgg) {
        super(source);
        this.fragment = fragment;
        this.esFilter = esFilter;
        this.estimatedRowSize = estimatedRowSize;
        this.singlePassAgg = singlePassAgg;
    }

    private FragmentExec(StreamInput in) throws IOException {
        super(Source.readFrom((PlanStreamInput) in));
        this.fragment = in.readNamedWriteable(LogicalPlan.class);
        this.esFilter = in.readOptionalNamedWriteable(QueryBuilder.class);
        this.estimatedRowSize = in.readVInt();
        this.singlePassAgg = in.getTransportVersion().supports(SINGLE_PASS_AGG_TV) && in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(fragment());
        out.writeOptionalNamedWriteable(esFilter());
        out.writeVInt(estimatedRowSize);
        if (out.getTransportVersion().supports(SINGLE_PASS_AGG_TV)) {
            out.writeBoolean(singlePassAgg);
        }
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
        return NodeInfo.create(this, FragmentExec::new, fragment, esFilter, estimatedRowSize, singlePassAgg);
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
            : new FragmentExec(source(), fragment, esFilter, estimatedRowSize, singlePassAgg);
    }

    public FragmentExec withFragment(LogicalPlan fragment) {
        return Objects.equals(fragment, this.fragment)
            ? this
            : new FragmentExec(source(), fragment, esFilter, estimatedRowSize, singlePassAgg);
    }

    public FragmentExec withFilter(QueryBuilder filter) {
        return Objects.equals(filter, this.esFilter) ? this : new FragmentExec(source(), fragment, filter, estimatedRowSize, singlePassAgg);
    }

    public FragmentExec withSinglePassAgg(boolean singlePassAgg) {
        return this.singlePassAgg == singlePassAgg ? this : new FragmentExec(source(), fragment, esFilter, estimatedRowSize, singlePassAgg);
    }

    public boolean singlePassAgg() {
        return singlePassAgg;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fragment, esFilter, estimatedRowSize, singlePassAgg);
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
            && singlePassAgg == other.singlePassAgg;
    }

    @Override
    public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
        sb.append(nodeName());
        // esFilter is a raw QueryBuilder DSL from request.filter() — opaque content; route it through
        // the opaque mapper so it prints raw under identity and redacts under anonymization.
        sb.append("[filter=").append(mapper.opaque(String.valueOf(esFilter)));
        sb.append(", estimatedRowSize=").append(estimatedRowSize);
        if (singlePassAgg) {
            sb.append(", singlePassAgg=true");
        }
        sb.append(", reducer=[], fragment=[<>\n");
        sb.append(fragment.toString(format, mapper));
        sb.append("<>]]");
    }

}
