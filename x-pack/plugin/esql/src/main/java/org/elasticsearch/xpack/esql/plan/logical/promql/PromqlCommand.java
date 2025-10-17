/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

/**
 * Container plan for embedded PromQL queries.
 * Gets eliminated by the analyzer once the query is validated.
 */
public class PromqlCommand extends UnaryPlan implements TelemetryAware {

    private final LogicalPlan promqlPlan;
    private final Expression start, stop, step;

    // Instant query constructor - shortcut for a range constructor
    public PromqlCommand(Source source, LogicalPlan child, LogicalPlan promqlPlan, Expression time) {
        this(source, child, promqlPlan, time, time, Literal.timeDuration(source, Duration.ZERO));
    }

    // Range query constructor
    public PromqlCommand(Source source, LogicalPlan child, LogicalPlan promqlPlan, Expression start, Expression stop, Expression step) {
        super(source, child);
        this.promqlPlan = promqlPlan;
        this.start = start;
        this.stop = stop;
        this.step = step;
    }

    @Override
    protected NodeInfo<PromqlCommand> info() {
        return NodeInfo.create(this, PromqlCommand::new, child(), promqlPlan(), start(), stop(), step());
    }

    @Override
    public PromqlCommand replaceChild(LogicalPlan newChild) {
        return new PromqlCommand(source(), newChild, promqlPlan(), start(), stop(), step());
    }

    public PromqlCommand withPromqlPlan(LogicalPlan newPromqlPlan) {
        return new PromqlCommand(source(), child(), newPromqlPlan, start(), stop(), step());
    }

    @Override
    public boolean expressionsResolved() {
        return promqlPlan.resolved();
    }

    @Override
    public String telemetryLabel() {
        return "PROMQL";
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("serialization not supported");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("serialization not supported");
    }

    public LogicalPlan promqlPlan() {
        return promqlPlan;
    }

    public Expression start() {
        return start;
    }

    public Expression stop() {
        return stop;
    }

    public Expression step() {
        return step;
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), start, stop, step, promqlPlan);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {

        PromqlCommand other = (PromqlCommand) obj;
        return Objects.equals(child(), other.child()) && Objects.equals(promqlPlan, other.promqlPlan);
    }

        return false;
    }

    @Override
    public String nodeString() {
        StringBuilder sb = new StringBuilder();
        sb.append(nodeName());
        if (start == stop) {
            sb.append("time=").append(start);
        } else {
            sb.append("start=").append(start).append(", stop=").append(stop).append(", step=").append(step);
        }
        sb.append(" promql=[<>\n");
        sb.append(promqlPlan.toString());
        sb.append("\n<>]]");
        return sb.toString();
    }
}
