/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.explain;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * One invocation of the cost-based optimizer on one logical plan. A query can run the optimizer several times, once
 * initially and again after each executed subplan, and each run is reported separately because its decisions can differ.
 * {@link #planCost()} is the total cost of the chosen plan, shared by every decision in the run.
 */
public record RunReport(
    int run,
    Trigger trigger,
    @Nullable Integer subplan,
    RunStatus status,
    @Nullable String statusReason,
    @Nullable SearchStats search,
    @Nullable CostVector planCost,
    List<DecisionReport> decisions,
    int truncatedDecisions,
    @Nullable String explain
) implements Writeable, ToXContentObject {

    public RunReport {
        Objects.requireNonNull(trigger, "trigger");
        Objects.requireNonNull(status, "status");
        decisions = decisions == null ? List.of() : List.copyOf(decisions);
    }

    public RunReport(StreamInput in) throws IOException {
        this(
            in.readVInt(),
            in.readEnum(Trigger.class),
            in.readOptionalVInt(),
            in.readEnum(RunStatus.class),
            in.readOptionalString(),
            in.readOptionalWriteable(SearchStats::new),
            in.readOptionalWriteable(CostVector::new),
            in.readCollectionAsImmutableList(DecisionReport::new),
            in.readVInt(),
            in.readOptionalString()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(run);
        out.writeEnum(trigger);
        out.writeOptionalVInt(subplan);
        out.writeEnum(status);
        out.writeOptionalString(statusReason);
        out.writeOptionalWriteable(search);
        out.writeOptionalWriteable(planCost);
        out.writeCollection(decisions);
        out.writeVInt(truncatedDecisions);
        out.writeOptionalString(explain);
    }

    /** Returns a copy with a different run ordinal. */
    public RunReport withRun(int r) {
        return new RunReport(r, trigger, subplan, status, statusReason, search, planCost, decisions, truncatedDecisions, explain);
    }

    /** Returns a copy with different decisions, for example after outcomes were attached. */
    public RunReport withDecisions(List<DecisionReport> d) {
        return new RunReport(run, trigger, subplan, status, statusReason, search, planCost, d, truncatedDecisions, explain);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        b.startObject();
        b.field("run", run);
        b.field("trigger", trigger.displayName());
        if (subplan != null) {
            b.field("subplan", subplan.intValue());
        }
        b.field("status", status.displayName());
        ExplainXContent.optionalField(b, "status_reason", statusReason);
        ExplainXContent.optionalObject(b, "search", search, params);
        if (planCost != null) {
            planCost.toXContentObject(b, "plan_cost");
        }
        ExplainXContent.optionalArray(b, "decisions", decisions, params);
        if (truncatedDecisions > 0) {
            b.field("truncated_decisions", truncatedDecisions);
        }
        ExplainXContent.optionalField(b, "explain", explain);
        return b.endObject();
    }
}
