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
    static final ParseField CREATION_DATE = new ParseField("creation_date");
    static final ParseField CREATION_DATE_STRING = new ParseField("creation_date_string");

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
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), CREATION_DATE);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), CREATION_DATE_STRING);
    }

    public static LifecyclePolicyMetadata parse(XContentParser parser, String name) {
        return PARSER.apply(parser, name);
    }

    private final LifecyclePolicy policy;
    private final Map<String, String> headers;
    private final long version;
    private final long creationDate;

    public LifecyclePolicyMetadata(LifecyclePolicy policy, Map<String, String> headers, long version, long creationDate) {
        this.policy = policy;
        this.headers = headers;
        this.version = version;
        this.creationDate = creationDate;
    }

    @SuppressWarnings("unchecked")
    public LifecyclePolicyMetadata(StreamInput in) throws IOException {
        this.policy = new LifecyclePolicy(in);
        this.headers = (Map<String, String>) in.readGenericValue();
        this.version = in.readVLong();
        this.creationDate = in.readVLong();
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

    public long getCreationDate() {
        return creationDate;
    }

    public String getCreationDateString() {
        ZonedDateTime creationDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(creationDate), ZoneOffset.UTC);
        return creationDateTime.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(POLICY.getPreferredName(), policy);
        builder.field(HEADERS.getPreferredName(), headers);
        builder.field(VERSION.getPreferredName(), version);
        builder.field(CREATION_DATE.getPreferredName(), creationDate);
        builder.field(CREATION_DATE_STRING.getPreferredName(), getCreationDateString());
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        policy.writeTo(out);
        out.writeGenericValue(headers);
        out.writeVLong(version);
        out.writeVLong(creationDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(policy, headers, version, creationDate);
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
            Objects.equals(creationDate, other.creationDate);
    }

}
