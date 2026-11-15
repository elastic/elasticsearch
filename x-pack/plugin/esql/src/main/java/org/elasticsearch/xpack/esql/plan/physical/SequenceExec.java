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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Coordinator source of completed matches. Stage plans remain scoped logical inputs so the sequence reader, rather than
 * ordinary fragment planning, controls their shared snapshot, complete paging and source ordinals. Downstream physical
 * operators consume only the wide completed rows; their limits cannot truncate a candidate stage.
 */
public final class SequenceExec extends LeafExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "SequenceExec",
        SequenceExec::new
    );

    private final Sequence sequence;
    private final QueryBuilder esFilter;

    public SequenceExec(Source source, Sequence sequence) {
        this(source, sequence, null);
    }

    /** Carries the request's index filter into the snapshot searches hidden behind this coordinator leaf. */
    public SequenceExec(Source source, Sequence sequence, @Nullable QueryBuilder esFilter) {
        super(source);
        if (sequence.resolved() == false) {
            throw new IllegalArgumentException("SEQUENCE must be resolved before physical planning");
        }
        this.sequence = Objects.requireNonNull(sequence);
        this.esFilter = esFilter;
    }

    private SequenceExec(StreamInput in) throws IOException {
        this(readSource(in), (Sequence) in.readNamedWriteable(LogicalPlan.class), in.readOptionalNamedWriteable(QueryBuilder.class));
    }

    private static Source readSource(StreamInput in) throws IOException {
        Sequence.checkPlanSupported(in.getTransportVersion());
        return Source.readFrom((PlanStreamInput) in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Sequence.checkPlanSupported(out.getTransportVersion());
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(sequence);
        out.writeOptionalNamedWriteable(esFilter);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Sequence sequence() {
        return sequence;
    }

    public QueryBuilder esFilter() {
        return esFilter;
    }

    /** Preserves schema identity when the session integrates its out-of-band request filter. */
    public SequenceExec withFilter(QueryBuilder filter) {
        return new SequenceExec(source(), sequence, filter);
    }

    @Override
    public List<Attribute> output() {
        return sequence.output();
    }

    @Override
    protected NodeInfo<SequenceExec> info() {
        return NodeInfo.create(this, SequenceExec::new, sequence, esFilter);
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj
            || obj instanceof SequenceExec other && sequence.equals(other.sequence) && Objects.equals(esFilter, other.esFilter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sequence, esFilter);
    }
}
