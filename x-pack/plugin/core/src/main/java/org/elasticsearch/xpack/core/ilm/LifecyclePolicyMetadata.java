/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Objects;

public class LifecyclePolicyMetadata extends AbstractDiffable<LifecyclePolicyMetadata>
        implements ToXContentObject, Diffable<LifecyclePolicyMetadata> {

    static final ParseField POLICY = new ParseField("policy");
    static final ParseField HEADERS = new ParseField("headers");
    static final ParseField VERSION = new ParseField("version");
    static final ParseField MODIFIED_DATE = new ParseField("modified_date");
    static final ParseField MODIFIED_DATE_STRING = new ParseField("modified_date_string");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<LifecyclePolicyMetadata, String> PARSER = new ConstructingObjectParser<>("policy_metadata",
            a -> {
                LifecyclePolicy policy = (LifecyclePolicy) a[0];
                return new LifecyclePolicyMetadata(policy, (Map<String, String>) a[1], (long) a[2], (long) a[3]);
            });
    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), LifecyclePolicy::parse, POLICY);
        PARSER.declareField(ConstructingObjectParser.constructorArg(), XContentParser::mapStrings, HEADERS, ValueType.OBJECT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), VERSION);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MODIFIED_DATE);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), MODIFIED_DATE_STRING);
    }

    public static LifecyclePolicyMetadata parse(XContentParser parser, String name) {
        return PARSER.apply(parser, name);
    }

    private final LifecyclePolicy policy;
    private final Map<String, String> headers;
    private final long version;
    private final long modifiedDate;

    public LifecyclePolicyMetadata(LifecyclePolicy policy, Map<String, String> headers, long version, long modifiedDate) {
        this.policy = policy;
        this.headers = headers;
        this.version = version;
        this.modifiedDate = modifiedDate;
    }

    @SuppressWarnings("unchecked")
    public LifecyclePolicyMetadata(StreamInput in) throws IOException {
        this.policy = new LifecyclePolicy(in);
        this.headers = (Map<String, String>) in.readGenericValue();
        this.version = in.readVLong();
        this.modifiedDate = in.readVLong();
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

    public long getVersion() {
        return version;
    }

    public long getModifiedDate() {
        return modifiedDate;
    }

    public String getModifiedDateString() {
        ZonedDateTime modifiedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(modifiedDate), ZoneOffset.UTC);
        return modifiedDateTime.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(POLICY.getPreferredName(), policy);
        builder.field(HEADERS.getPreferredName(), headers);
        builder.field(VERSION.getPreferredName(), version);
        builder.field(MODIFIED_DATE.getPreferredName(), modifiedDate);
        builder.field(MODIFIED_DATE_STRING.getPreferredName(), getModifiedDateString());
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        policy.writeTo(out);
        out.writeGenericValue(headers);
        out.writeVLong(version);
        out.writeVLong(modifiedDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(policy, headers, version, modifiedDate);
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
            Objects.equals(headers, other.headers) &&
            Objects.equals(version, other.version) &&
            Objects.equals(modifiedDate, other.modifiedDate);
    }

}
