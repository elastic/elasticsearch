/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class IndexLifecycleFeatureSetUsage extends XPackFeatureSet.Usage {

    private List<PolicyStats> policyStats;

    public IndexLifecycleFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        policyStats = input.readList(PolicyStats::new);
    }

    public IndexLifecycleFeatureSetUsage(boolean available, boolean enabled) {
        this(available, enabled, null);
    }

    public IndexLifecycleFeatureSetUsage(boolean available, boolean enabled, List<PolicyStats> policyStats) {
        super(XPackField.INDEX_LIFECYCLE, available, enabled);
        this.policyStats = policyStats;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        if (policyStats != null) {
            builder.field("policy_count", policyStats.size());
            builder.field("policy_stats", policyStats);
        }
    }

    public static final class PolicyStats implements ToXContentObject, Writeable {
        private final Map<String, PhaseStats> phaseStats;

        public PolicyStats(Map<String, PhaseStats> phaseStats) {
            this.phaseStats = phaseStats;
        }

        public PolicyStats(StreamInput in) throws IOException {
            this.phaseStats = in.readMap(StreamInput::readString, PhaseStats::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(phaseStats, StreamOutput::writeString, (o, p) -> p.writeTo(o));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(LifecyclePolicy.PHASES_FIELD.getPreferredName(), phaseStats);
            builder.endObject();
            return builder;
        }
    }

    public static final class PhaseStats implements ToXContentObject, Writeable {
        private final String[] actionNames;
        private final TimeValue after;

        public PhaseStats(TimeValue after, String[] actionNames) {
            this.actionNames = actionNames;
            this.after = after;
        }

        public PhaseStats(StreamInput in) throws IOException {
            actionNames = in.readStringArray();
            after = in.readTimeValue();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringArray(actionNames);
            out.writeTimeValue(after);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(Phase.AFTER_FIELD.getPreferredName(), after.getStringRep());
            builder.field(Phase.ACTIONS_FIELD.getPreferredName(), actionNames);
            builder.endObject();
            return builder;
        }
    }
}
