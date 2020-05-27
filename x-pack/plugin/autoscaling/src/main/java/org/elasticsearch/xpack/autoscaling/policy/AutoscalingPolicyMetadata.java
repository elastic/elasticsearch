/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.policy;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class AutoscalingPolicyMetadata extends AbstractDiffable<AutoscalingPolicyMetadata>
    implements
        Diffable<AutoscalingPolicyMetadata>,
        ToXContentObject {

    static final ParseField POLICY_FIELD = new ParseField("policy");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<AutoscalingPolicyMetadata, String> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>("autoscaling_policy_metadata", a -> {
            final AutoscalingPolicy policy = (AutoscalingPolicy) a[0];
            return new AutoscalingPolicyMetadata(policy);
        });
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), AutoscalingPolicy::parse, POLICY_FIELD);
    }

    public static AutoscalingPolicyMetadata parse(final XContentParser parser, final String name) {
        return PARSER.apply(parser, name);
    }

    private final AutoscalingPolicy policy;

    public AutoscalingPolicy policy() {
        return policy;
    }

    public AutoscalingPolicyMetadata(final AutoscalingPolicy policy) {
        this.policy = policy;
    }

    public AutoscalingPolicyMetadata(final StreamInput in) throws IOException {
        policy = new AutoscalingPolicy(in);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        policy.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        {
            builder.field(POLICY_FIELD.getPreferredName(), policy);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final AutoscalingPolicyMetadata that = (AutoscalingPolicyMetadata) o;
        return policy.equals(that.policy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(policy);
    }

}
