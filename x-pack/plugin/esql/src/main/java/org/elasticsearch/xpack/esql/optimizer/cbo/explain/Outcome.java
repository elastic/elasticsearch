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
 * What the final physical plan did with a decision, cross-checked against the plan itself so that a decision that was
 * planned but not executed shows up in the report.
 * {@link #effective()} names the alternative that actually ran and is left out when it equals the chosen one.
 */
public record Outcome(
    OutcomeStatus status,
    @Nullable String effective,
    @Nullable String reason,
    @Nullable String physicalNode,
    @Nullable String observedIn
) implements Writeable, ToXContentObject {

    public Outcome {
        Objects.requireNonNull(status, "status");
    }

    public Outcome(StreamInput in) throws IOException {
        this(
            in.readEnum(OutcomeStatus.class),
            in.readOptionalString(),
            in.readOptionalString(),
            in.readOptionalString(),
            in.readOptionalString()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(status);
        out.writeOptionalString(effective);
        out.writeOptionalString(reason);
        out.writeOptionalString(physicalNode);
        out.writeOptionalString(observedIn);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder b, Params params) throws IOException {
        b.startObject();
        b.field("status", status.displayName());
        ExplainXContent.optionalField(b, "effective", effective);
        ExplainXContent.optionalField(b, "reason", reason);
        ExplainXContent.optionalField(b, "physical_node", physicalNode);
        ExplainXContent.optionalField(b, "observed_in", observedIn);
        return b.endObject();
    }
}
