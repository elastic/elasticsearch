/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.io.IOException;
import java.util.Objects;

public class Rerank extends UnaryPlan {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Rerank", Rerank::new);

    private static final Literal DEFAULT_WINDOW_SIZE = new Literal(Source.EMPTY, 20, DataType.INTEGER);

    private final Expression queryText;
    private final Expression input;
    private final Expression inferenceId;
    private final Expression windowSize;

    public Rerank(Source source, LogicalPlan child, Expression queryText, Expression input, Expression inferenceId, Expression windowSize) {
        super(source, child);
        this.queryText = queryText;
        this.input = input;
        this.inferenceId = inferenceId;
        this.windowSize = Objects.requireNonNullElse(windowSize, DEFAULT_WINDOW_SIZE);
    }

    public Rerank(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(queryText);
        out.writeNamedWriteable(input);
        out.writeNamedWriteable(inferenceId);
        out.writeNamedWriteable(windowSize);
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
    public String commandName() {
        return "RERANK";
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Rerank(source(), newChild, queryText, input, inferenceId, windowSize);
    }

    @Override
    public boolean expressionsResolved() {
        return queryText.resolved() && input.resolved() && inferenceId.resolved() && windowSize.resolved();
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Rerank::new, child(), queryText, input, inferenceId, windowSize);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        Rerank rerank = (Rerank) o;
        return Objects.equals(queryText, rerank.queryText)
            && Objects.equals(input, rerank.input)
            && Objects.equals(inferenceId, rerank.inferenceId)
            && Objects.equals(windowSize, rerank.windowSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), queryText, input, inferenceId, windowSize);
    }
}
