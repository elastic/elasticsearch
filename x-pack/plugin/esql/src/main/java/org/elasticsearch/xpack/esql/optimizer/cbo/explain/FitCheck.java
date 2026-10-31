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
import java.util.Objects;

/**
 * A memory-safety test applied to an alternative, so a reader can see how close an alternative came to being pruned.
 * {@link #probability()} and {@link #threshold()} are only set for {@link FitMethod#PROBABILITY} checks.
 */
public record FitCheck(
    String check,
    FitMethod method,
    EstimateView demand,
    long budgetInBytes,
    @Nullable Double probability,
    @Nullable Double threshold,
    boolean passed
) implements Writeable, ToXContentObject {

    public FitCheck {
        Objects.requireNonNull(check, "check");
        Objects.requireNonNull(method, "method");
        Objects.requireNonNull(demand, "demand");
    }

    public FitCheck(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readEnum(FitMethod.class),
            new EstimateView(in),
            in.readZLong(),
            in.readOptionalDouble(),
            in.readOptionalDouble(),
            in.readBoolean()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(check);
        out.writeEnum(method);
        demand.writeTo(out);
        out.writeZLong(budgetInBytes);
        out.writeOptionalDouble(probability);
        out.writeOptionalDouble(threshold);
        out.writeBoolean(passed);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        b.startObject();
        b.field("check", check);
        b.field("method", method.displayName());
        b.field("demand");
        demand.toXContent(b, params);
        b.field("budget_in_bytes", budgetInBytes);
        if (probability != null) {
            ExplainXContent.numberField(b, "probability", probability);
        }
        if (threshold != null) {
            ExplainXContent.numberField(b, "threshold", threshold);
        }
        b.field("passed", passed);
        return b.endObject();
    }
}
