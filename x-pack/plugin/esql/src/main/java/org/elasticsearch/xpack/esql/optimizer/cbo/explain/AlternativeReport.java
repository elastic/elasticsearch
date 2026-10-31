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
 * An enumerated alternative that a decision did not pick, with the evidence for why it lost.
 * {@link #planCost()} is the total cost of the best complete plan that uses this alternative, which can differ from the
 * chosen plan at other decision points too; {@link #otherDecisions()} lists those differences.
 */
public record AlternativeReport(
    String name,
    AlternativeStatus status,
    @Nullable String delivered,
    @Nullable CostVector planCost,
    @Nullable Delta delta,
    List<CostComponent> components,
    List<FitCheck> fitChecks,
    List<DecisionRef> otherDecisions,
    @Nullable TieBreak tieBreak
) implements Writeable, ToXContentObject {

    public AlternativeReport {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(status, "status");
        components = components == null ? List.of() : List.copyOf(components);
        fitChecks = fitChecks == null ? List.of() : List.copyOf(fitChecks);
        otherDecisions = otherDecisions == null ? List.of() : List.copyOf(otherDecisions);
    }

    public AlternativeReport(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readEnum(AlternativeStatus.class),
            in.readOptionalString(),
            in.readOptionalWriteable(CostVector::new),
            in.readOptionalWriteable(Delta::new),
            in.readCollectionAsImmutableList(CostComponent::new),
            in.readCollectionAsImmutableList(FitCheck::new),
            in.readCollectionAsImmutableList(DecisionRef::new),
            in.readOptionalWriteable(TieBreak::new)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeEnum(status);
        out.writeOptionalString(delivered);
        out.writeOptionalWriteable(planCost);
        out.writeOptionalWriteable(delta);
        out.writeCollection(components);
        out.writeCollection(fitChecks);
        out.writeCollection(otherDecisions);
        out.writeOptionalWriteable(tieBreak);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        b.startObject();
        b.field("name", name);
        b.field("status", status.displayName());
        ExplainXContent.optionalField(b, "delivered", delivered);
        if (planCost != null) {
            planCost.toXContentObject(b, "plan_cost");
        }
        ExplainXContent.optionalObject(b, "delta", delta, params);
        ExplainXContent.optionalArray(b, "components", components, params);
        ExplainXContent.optionalArray(b, "fit_checks", fitChecks, params);
        ExplainXContent.optionalArray(b, "other_decisions", otherDecisions, params);
        ExplainXContent.optionalObject(b, "tie_break", tieBreak, params);
        return b.endObject();
    }
}
