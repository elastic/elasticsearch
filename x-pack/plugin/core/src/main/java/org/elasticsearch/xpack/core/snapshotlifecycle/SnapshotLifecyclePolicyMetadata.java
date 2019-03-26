/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.snapshotlifecycle;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Objects;

/**
 * {@code SnapshotLifecyclePolicyMetadata} encapsulates a {@link SnapshotLifecyclePolicy} as well as
 * the additional meta information link headers used for execution, version (a monotonically
 * incrementing number), and last modified date
 */
public class SnapshotLifecyclePolicyMetadata extends AbstractDiffable<SnapshotLifecyclePolicyMetadata>
    implements ToXContentObject, Diffable<SnapshotLifecyclePolicyMetadata> {

    static final ParseField POLICY = new ParseField("policy");
    static final ParseField HEADERS = new ParseField("headers");
    static final ParseField VERSION = new ParseField("version");
    static final ParseField MODIFIED_DATE = new ParseField("modified_date");
    static final ParseField MODIFIED_DATE_STRING = new ParseField("modified_date_string");

    private final SnapshotLifecyclePolicy policy;
    private final Map<String, String> headers;
    private final long version;
    private final long modifiedDate;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<SnapshotLifecyclePolicyMetadata, String> PARSER =
        new ConstructingObjectParser<>("snapshot_policy_metadata",
            a -> {
                SnapshotLifecyclePolicy policy = (SnapshotLifecyclePolicy) a[0];
                return new SnapshotLifecyclePolicyMetadata(policy, (Map<String, String>) a[1], (long) a[2], (long) a[3]);
            });

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), SnapshotLifecyclePolicy::parse, POLICY);
        PARSER.declareField(ConstructingObjectParser.constructorArg(), XContentParser::mapStrings, HEADERS, ObjectParser.ValueType.OBJECT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), VERSION);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MODIFIED_DATE);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), MODIFIED_DATE_STRING);
    }

    public static SnapshotLifecyclePolicyMetadata parse(XContentParser parser, String name) {
        return PARSER.apply(parser, name);
    }

    public SnapshotLifecyclePolicyMetadata(SnapshotLifecyclePolicy policy, Map<String, String> headers, long version, long modifiedDate) {
        this.policy = policy;
        this.headers = headers;
        this.version = version;
        this.modifiedDate = modifiedDate;
    }

    @SuppressWarnings("unchecked")
    SnapshotLifecyclePolicyMetadata(StreamInput in) throws IOException {
        this.policy = new SnapshotLifecyclePolicy(in);
        this.headers = (Map<String, String>) in.readGenericValue();
        this.version = in.readVLong();
        this.modifiedDate = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.policy.writeTo(out);
        out.writeGenericValue(this.headers);
        out.writeVLong(this.version);
        out.writeVLong(this.modifiedDate);
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public SnapshotLifecyclePolicy getPolicy() {
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
        SnapshotLifecyclePolicyMetadata other = (SnapshotLifecyclePolicyMetadata) obj;
        return Objects.equals(policy, other.policy) &&
            Objects.equals(headers, other.headers) &&
            Objects.equals(version, other.version) &&
            Objects.equals(modifiedDate, other.modifiedDate);
    }

    @Override
    public String toString() {
        // Note: this is on purpose. While usually we would use Strings.toString(this) to render
        // this using toXContent, it may contain sensitive information in the headers and thus
        // should not emit them in case it accidentally gets logged.
        return super.toString();
    }
}
