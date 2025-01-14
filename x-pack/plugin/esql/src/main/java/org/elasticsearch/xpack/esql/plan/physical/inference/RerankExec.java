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

    private final Expression queryText;

    private final Expression input;
    private final Expression inferenceId;
    private final Expression windowSize;

    public RerankExec(Source source, PhysicalPlan child, Expression queryText, Expression input, Expression inferenceId, Expression windowSize) {
        super(source, child);
        this.queryText = queryText;
        this.input = input;
        this.inferenceId = inferenceId;
        this.windowSize = windowSize;
    }

    public RerankExec(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(PhysicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class)
        );
    }

    public Expression queryText() {
        return queryText;
    }

    public Expression input() {
        return input;
    }

    public Expression inferenceId() {
        return inferenceId;
    }

    public Expression windowSize() {
        return windowSize;
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(queryText());
        out.writeNamedWriteable(input());
        out.writeNamedWriteable(inferenceId());
        out.writeNamedWriteable(windowSize());
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, RerankExec::new, child(), queryText, input, inferenceId, windowSize);
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new RerankExec(source(), newChild, queryText, input, inferenceId, windowSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), queryText, input, inferenceId, windowSize);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        RerankExec rerankExec = (RerankExec) o;

        return Objects.equals(queryText, rerankExec.queryText)
            && Objects.equals(input, rerankExec.input)
            && Objects.equals(inferenceId, rerankExec.inferenceId)
            && Objects.equals(windowSize, rerankExec.windowSize);
    }
}
