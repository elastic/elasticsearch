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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical counterpart of {@link org.elasticsearch.xpack.esql.plan.logical.eql.EqlQuery}. Executes coordinator-only:
 * the {@code LocalExecutionPlanner} turns it into a source operator that calls the EQL search endpoint and streams the
 * response back as pages.
 * <p>
 * The optional {@code limit} is the ES|QL {@code LIMIT} pushed down onto the source; it is forwarded to the EQL request
 * as {@code size}. Because the whole command is snapshot-gated ({@code Cap.EQL_QUERY}) and brand new, {@code limit} is
 * serialized alongside the other fields without a dedicated {@code TransportVersion}; a version gate must be added before
 * the feature is released/backported (see {@code AGENTS.md} "Backwards compatibility").
 */
public class EqlQueryExec extends LeafExec {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "EqlQueryExec",
        EqlQueryExec::new
    );

    private final String index;
    private final String query;
    private final List<Attribute> output;
    @Nullable
    private final Integer limit;

    public EqlQueryExec(Source source, String index, String query, List<Attribute> output, @Nullable Integer limit) {
        super(source);
        this.index = index;
        this.query = query;
        this.output = output;
        this.limit = limit;
    }

    private EqlQueryExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readString(),
            in.readString(),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.readOptionalInt()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Physical plan nodes do not preserve Source on the wire (see ShowExec); it round-trips as Source.EMPTY.
        Source.EMPTY.writeTo(out);
        out.writeString(index);
        out.writeString(query);
        out.writeNamedWriteableCollection(output);
        out.writeOptionalInt(limit);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public String index() {
        return index;
    }

    public String query() {
        return query;
    }

    @Nullable
    public Integer limit() {
        return limit;
    }

    @Override
    public List<Attribute> output() {
        return output;
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, EqlQueryExec::new, index, query, output, limit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, query, output, limit);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        EqlQueryExec other = (EqlQueryExec) obj;
        return Objects.equals(index, other.index)
            && Objects.equals(query, other.query)
            && Objects.equals(output, other.output)
            && Objects.equals(limit, other.limit);
    }
}
