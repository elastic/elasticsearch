/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.enrich.action;

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.tasks.Task;

public class ExecuteEnrichPolicyStatus implements Task.Status {

    public static final class PolicyPhases {
        private PolicyPhases() {}

        public static final String SCHEDULED = "SCHEDULED";
        public static final String RUNNING = "RUNNING";
        public static final String COMPLETE = "COMPLETE";
        public static final String FAILED = "FAILED";
    }

    public static final String NAME = "enrich-policy-execution";

    private static final String PHASE_FIELD = "phase";

    private final String phase;

    public ExecuteEnrichPolicyStatus(String phase) {
        this.phase = phase;
    }

    public ExecuteEnrichPolicyStatus(StreamInput in) throws IOException {
        this.phase = in.readString();
    }

    public String getPhase() {
        return phase;
    }

    public boolean isCompleted() {
        return PolicyPhases.COMPLETE.equals(phase);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(phase);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field(PHASE_FIELD, phase);
        }
        builder.endObject();
        return builder;
    }
}
