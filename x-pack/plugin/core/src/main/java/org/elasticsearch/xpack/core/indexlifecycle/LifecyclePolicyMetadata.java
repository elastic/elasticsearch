/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class LifecyclePolicyMetadata extends AbstractDiffable<LifecyclePolicyMetadata>
        implements ToXContentObject, Diffable<LifecyclePolicyMetadata> {

    public static final ParseField POLICY = new ParseField("policy");
    public static final ParseField HEADERS = new ParseField("headers");
    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<LifecyclePolicyMetadata, String> PARSER = new ConstructingObjectParser<>("policy_metadata",
            a -> {
                LifecyclePolicy policy = (LifecyclePolicy) a[0];
                return new LifecyclePolicyMetadata(policy, (Map<String, String>) a[1]);
            });
    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), LifecyclePolicy::parse, POLICY);
        PARSER.declareField(ConstructingObjectParser.constructorArg(), XContentParser::mapStrings, HEADERS, ValueType.OBJECT);
    }

    public static LifecyclePolicyMetadata parse(XContentParser parser, String name) {
        return PARSER.apply(parser, name);
    }

    private final LifecyclePolicy policy;
    private final Map<String, String> headers;

    public LifecyclePolicyMetadata(LifecyclePolicy policy, Map<String, String> headers) {
        this.policy = policy;
        this.headers = headers;
    }

    @SuppressWarnings("unchecked")
    public LifecyclePolicyMetadata(StreamInput in) throws IOException {
        this.policy = new LifecyclePolicy(in);
        this.headers = (Map<String, String>) in.readGenericValue();
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public LifecyclePolicy getPolicy() {
        return policy;
    }

    public String getName() {
        return policy.getName();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(POLICY.getPreferredName(), policy);
        builder.field(HEADERS.getPreferredName(), headers);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        policy.writeTo(out);
        out.writeGenericValue(headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(policy, headers);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        LifecyclePolicyMetadata other = (LifecyclePolicyMetadata) obj;
        return Objects.equals(policy, other.policy) &&
            Objects.equals(headers, other.headers);
    }

}
