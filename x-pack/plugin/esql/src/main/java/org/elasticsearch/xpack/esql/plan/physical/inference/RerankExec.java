/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;

import java.io.IOException;
import java.util.Objects;

public class RerankExec extends UnaryExec {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "RerankExec",
        RerankExec::new
    );

    private final String inferenceId;
    private final String queryText;
    private final Expression input;

    public RerankExec(Source source, PhysicalPlan child, String inferenceId, String queryText, Expression input) {
        super(source, child);
        this.queryText = queryText;
        this.input = input;
        this.inferenceId = inferenceId;
    }

    public RerankExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readString(),
            in.readString(),
            in.readNamedWriteable(Expression.class)
        );
    }

    public String inferenceId() {
        return inferenceId;
    }

    public String queryText() {
        return queryText;
    }

    public Expression input() {
        return input;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeString(inferenceId());
        out.writeString(queryText());
        out.writeNamedWriteable(input());
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, RerankExec::new, child(), inferenceId, queryText, input);
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new RerankExec(source(), newChild, inferenceId, queryText, input);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), queryText, input, inferenceId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        RerankExec rerankExec = (RerankExec) o;

        return Objects.equals(queryText, rerankExec.queryText)
            && Objects.equals(input, rerankExec.input)
            && Objects.equals(inferenceId, rerankExec.inferenceId);
    }
}
