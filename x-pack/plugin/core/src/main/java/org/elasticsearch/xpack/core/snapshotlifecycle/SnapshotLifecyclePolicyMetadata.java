/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.snapshotlifecycle;

import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.common.Nullable;
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
    static final ParseField LAST_SUCCESS_DATE = new ParseField("last_success_date");
    static final ParseField LAST_SUCCESS_DATE_STRING = new ParseField("last_success_date_string");
    static final ParseField LAST_FAILURE_DATE = new ParseField("last_failure_date");
    static final ParseField LAST_FAILURE_DATE_STRING = new ParseField("last_failure_date_string");
    static final ParseField LAST_FAILURE_INFO = new ParseField("last_failure_information");

    private final SnapshotLifecyclePolicy policy;
    private final Map<String, String> headers;
    private final long version;
    private final long modifiedDate;
    @Nullable
    private final Long lastSuccessDate;
    @Nullable
    private final Long lastFailureDate;
    @Nullable
    private final String lastFailureInfo;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<SnapshotLifecyclePolicyMetadata, String> PARSER =
        new ConstructingObjectParser<>("snapshot_policy_metadata",
            a -> {
                SnapshotLifecyclePolicy policy = (SnapshotLifecyclePolicy) a[0];
                return new Builder().setPolicy(policy).setHeaders((Map<String, String>) a[1]).setVersion((long) a[2]).setModifiedDate((long) a[3]).setLastSuccessDate((Long) a[4]).setLastFailureDate((Long) a[5]).setLastFailureInfo((String) a[6]).build();
            });

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), SnapshotLifecyclePolicy::parse, POLICY);
        PARSER.declareField(ConstructingObjectParser.constructorArg(), XContentParser::mapStrings, HEADERS, ObjectParser.ValueType.OBJECT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), VERSION);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MODIFIED_DATE);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), MODIFIED_DATE_STRING);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), LAST_SUCCESS_DATE);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), LAST_SUCCESS_DATE_STRING);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), LAST_FAILURE_DATE);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), LAST_FAILURE_DATE_STRING);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), LAST_FAILURE_INFO);
    }

    public static SnapshotLifecyclePolicyMetadata parse(XContentParser parser, String name) {
        return PARSER.apply(parser, name);
    }

    SnapshotLifecyclePolicyMetadata(SnapshotLifecyclePolicy policy, Map<String, String> headers, long version, long modifiedDate,
                                           Long lastSuccessDate, Long lastFailureDate, String lastFailureInfo) {
        this.policy = Objects.requireNonNull(policy);
        this.headers = Objects.requireNonNull(headers);
        this.version = version;
        this.modifiedDate = modifiedDate;
        this.lastSuccessDate = lastSuccessDate;
        this.lastFailureDate = lastFailureDate;
        this.lastFailureInfo = lastFailureInfo;
    }

    @SuppressWarnings("unchecked")
    SnapshotLifecyclePolicyMetadata(StreamInput in) throws IOException {
        this.policy = new SnapshotLifecyclePolicy(in);
        this.headers = (Map<String, String>) in.readGenericValue();
        this.version = in.readVLong();
        this.modifiedDate = in.readVLong();
        this.lastSuccessDate = in.readOptionalLong();
        this.lastFailureDate = in.readOptionalLong();
        this.lastFailureInfo = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.policy.writeTo(out);
        out.writeGenericValue(this.headers);
        out.writeVLong(this.version);
        out.writeVLong(this.modifiedDate);
        out.writeOptionalLong(this.lastSuccessDate);
        out.writeOptionalLong(this.lastFailureDate);
        out.writeOptionalString(this.lastFailureInfo);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(SnapshotLifecyclePolicyMetadata metadata) {
        if (metadata == null) {
            return builder();
        }
        return new Builder()
            .setHeaders(metadata.getHeaders())
            .setPolicy(metadata.getPolicy())
            .setVersion(metadata.getVersion())
            .setModifiedDate(metadata.getModifiedDate())
            .setLastSuccessDate(metadata.getLastSuccessDate())
            .setLastFailureDate(metadata.getLastFailureDate())
            .setLastFailureInfo(metadata.getLastFailureInfo());
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

    private String dateToDateString(Long date) {
        if (Objects.isNull(date)) {
            return null;
        }
        ZonedDateTime dateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(date), ZoneOffset.UTC);
        return dateTime.toString();
    }

    public String getModifiedDateString() {
        return dateToDateString(modifiedDate);
    }

    public Long getLastSuccessDate() {
        return lastSuccessDate;
    }

    public String getLastSuccessDateString() {
        return dateToDateString(lastSuccessDate);
    }

    public Long getLastFailureDate() {
        return lastFailureDate;
    }
    public String getLastFailureDateString() {
        return dateToDateString(lastFailureDate);
    }

    public String getLastFailureInfo() {
        return lastFailureInfo;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(POLICY.getPreferredName(), policy);
        builder.field(HEADERS.getPreferredName(), headers);
        builder.field(VERSION.getPreferredName(), version);
        builder.field(MODIFIED_DATE.getPreferredName(), modifiedDate);
        builder.field(MODIFIED_DATE_STRING.getPreferredName(), getModifiedDateString());
        builder.field(LAST_SUCCESS_DATE.getPreferredName(), lastSuccessDate);
        builder.field(LAST_SUCCESS_DATE_STRING.getPreferredName(), getLastSuccessDateString());
        builder.field(LAST_FAILURE_DATE.getPreferredName(), lastFailureDate);
        builder.field(LAST_FAILURE_DATE_STRING.getPreferredName(), getLastFailureDateString());
        builder.field(LAST_FAILURE_INFO.getPreferredName(), lastFailureInfo);
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
            Objects.equals(modifiedDate, other.modifiedDate) &&
            Objects.equals(lastSuccessDate, other.lastSuccessDate) &&
            Objects.equals(lastFailureDate, other.lastFailureDate) &&
            Objects.equals(lastFailureInfo, other.lastFailureInfo);
    }

    @Override
    public String toString() {
        // Note: this is on purpose. While usually we would use Strings.toString(this) to render
        // this using toXContent, it may contain sensitive information in the headers and thus
        // should not emit them in case it accidentally gets logged.
        return super.toString();
    }

    public static class Builder {

        private Builder() {}

        private SnapshotLifecyclePolicy policy;
        private Map<String, String> headers;
        private long version = 1L;
        private Long modifiedDate;
        private Long lastSuccessDate;
        private Long lastFailureDate;
        private String lastFailureInfo;

        public Builder setPolicy(SnapshotLifecyclePolicy policy) {
            this.policy = policy;
            return this;
        }

        public Builder setHeaders(Map<String, String> headers) {
            this.headers = headers;
            return this;
        }

        public Builder setVersion(long version) {
            this.version = version;
            return this;
        }

        public Builder setModifiedDate(long modifiedDate) {
            this.modifiedDate = modifiedDate;
            return this;
        }

        public Builder setLastSuccessDate(Long lastSuccessDate) {
            this.lastSuccessDate = lastSuccessDate;
            return this;
        }

        public Builder setLastFailureDate(Long lastFailureDate) {
            this.lastFailureDate = lastFailureDate;
            return this;
        }

        public Builder setLastFailureInfo(String lastFailureInfo) {
            this.lastFailureInfo = lastFailureInfo;
            return this;
        }

        public SnapshotLifecyclePolicyMetadata build() {
            Objects.requireNonNull(modifiedDate, "modifiedDate must be set");
            return new SnapshotLifecyclePolicyMetadata(policy, headers, version, modifiedDate, lastSuccessDate, lastFailureDate, lastFailureInfo);
        }
    }
}
