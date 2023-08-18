/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.enrich.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class ExecuteEnrichPolicyStatus implements Task.Status {

    public static final class PolicyPhases {
        private PolicyPhases() {}

        public static final String SCHEDULED = "SCHEDULED";
        public static final String RUNNING = "RUNNING";
        public static final String COMPLETE = "COMPLETE";
        public static final String FAILED = "FAILED";
        public static final String CANCELLED = "CANCELLED";
    }

    public static final String NAME = "enrich-policy-execution";

    private static final String PHASE_FIELD = "phase";
    private static final String STEP_FIELD = "step";

    private final String phase;
    private final String step;

    public ExecuteEnrichPolicyStatus(String phase) {
        this.phase = phase;
        this.step = null;
    }

    public ExecuteEnrichPolicyStatus(ExecuteEnrichPolicyStatus status, String step) {
        this.phase = status.phase;
        this.step = step;
    }

    public ExecuteEnrichPolicyStatus(StreamInput in) throws IOException {
        this.phase = in.readString();
        this.step = in.getTransportVersion().onOrAfter(TransportVersion.V_7_16_0) ? in.readOptionalString() : null;
    }

    public String getPhase() {
        return phase;
    }

    public boolean isCompleted() {
        return PolicyPhases.COMPLETE.equals(phase);
    }

    public String getStep() {
        return step;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(phase);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_16_0)) {
            out.writeOptionalString(step);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(PHASE_FIELD, phase);
            if (step != null) {
                builder.field(STEP_FIELD, step);
            }
        }
        builder.endObject();
        return builder;
    }
}
