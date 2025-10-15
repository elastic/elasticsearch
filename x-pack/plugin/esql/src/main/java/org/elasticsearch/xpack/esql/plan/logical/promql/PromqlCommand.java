/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.io.IOException;
import java.util.Objects;

/**
 * Container plan for embedded PromQL queries.
 * Gets eliminated by the analyzer once the query is validated.
 */
public class PromqlCommand extends UnaryPlan implements TelemetryAware {

    private final LogicalPlan promqlPlan;

    public PromqlCommand(Source source, LogicalPlan child, LogicalPlan promqlPlan) {
        super(source, child);
        this.promqlPlan = promqlPlan;
    }

    @Override
    protected NodeInfo<PromqlCommand> info() {
        return NodeInfo.create(this, PromqlCommand::new, child(), promqlPlan);
    }

    @Override
    public PromqlCommand replaceChild(LogicalPlan newChild) {
        return new PromqlCommand(source(), newChild, promqlPlan);
    }

    public PromqlCommand withPromqlPlan(LogicalPlan newPromqlPlan) {
        return new PromqlCommand(source(), child(), newPromqlPlan);
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

    @Override
    public int hashCode() {
        return Objects.hash(child(), promqlPlan);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        PromqlCommand other = (PromqlCommand) obj;
        return Objects.equals(child(), other.child()) && Objects.equals(promqlPlan, other.promqlPlan);
    }

    @Override
    public String nodeString() {
        StringBuilder sb = new StringBuilder();
        sb.append(nodeName());
        sb.append(" promql=[<>\n");
        sb.append(promqlPlan.toString());
        sb.append("\n<>]]");
        return sb.toString();
    }
}
