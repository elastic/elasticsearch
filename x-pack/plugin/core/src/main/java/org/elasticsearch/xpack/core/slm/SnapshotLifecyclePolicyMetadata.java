/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.slm;

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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
    static final ParseField MODIFIED_DATE_MILLIS = new ParseField("modified_date_millis");
    static final ParseField MODIFIED_DATE = new ParseField("modified_date");
    static final ParseField LAST_SUCCESS = new ParseField("last_success");
    static final ParseField LAST_FAILURE = new ParseField("last_failure");
    static final ParseField NEXT_EXECUTION_MILLIS = new ParseField("next_execution_millis");
    static final ParseField NEXT_EXECUTION = new ParseField("next_execution");

    private final SnapshotLifecyclePolicy policy;
    private final Map<String, String> headers;
    private final long version;
    private final long modifiedDate;
    @Nullable
    private final SnapshotInvocationRecord lastSuccess;
    @Nullable
    private final SnapshotInvocationRecord lastFailure;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<SnapshotLifecyclePolicyMetadata, String> PARSER =
        new ConstructingObjectParser<>("snapshot_policy_metadata",
            a -> {
                SnapshotLifecyclePolicy policy = (SnapshotLifecyclePolicy) a[0];
                SnapshotInvocationRecord lastSuccess = (SnapshotInvocationRecord) a[4];
                SnapshotInvocationRecord lastFailure = (SnapshotInvocationRecord) a[5];

                return builder()
                    .setPolicy(policy)
                    .setHeaders((Map<String, String>) a[1])
                    .setVersion((long) a[2])
                    .setModifiedDate((long) a[3])
                    .setLastSuccess(lastSuccess)
                    .setLastFailure(lastFailure)
                    .build();
            });

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), SnapshotLifecyclePolicy::parse, POLICY);
        PARSER.declareField(ConstructingObjectParser.constructorArg(), XContentParser::mapStrings, HEADERS, ObjectParser.ValueType.OBJECT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), VERSION);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MODIFIED_DATE_MILLIS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), SnapshotInvocationRecord::parse, LAST_SUCCESS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), SnapshotInvocationRecord::parse, LAST_FAILURE);
    }

    public static SnapshotLifecyclePolicyMetadata parse(XContentParser parser, String name) {
        return PARSER.apply(parser, name);
    }

    SnapshotLifecyclePolicyMetadata(SnapshotLifecyclePolicy policy, Map<String, String> headers, long version, long modifiedDate,
                                    SnapshotInvocationRecord lastSuccess, SnapshotInvocationRecord lastFailure) {
        this.policy = policy;
        this.headers = headers;
        this.version = version;
        this.modifiedDate = modifiedDate;
        this.lastSuccess = lastSuccess;
        this.lastFailure = lastFailure;
    }

    @SuppressWarnings("unchecked")
    SnapshotLifecyclePolicyMetadata(StreamInput in) throws IOException {
        this.policy = new SnapshotLifecyclePolicy(in);
        this.headers = (Map<String, String>) in.readGenericValue();
        this.version = in.readVLong();
        this.modifiedDate = in.readVLong();
        this.lastSuccess = in.readOptionalWriteable(SnapshotInvocationRecord::new);
        this.lastFailure = in.readOptionalWriteable(SnapshotInvocationRecord::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.policy.writeTo(out);
        out.writeGenericValue(this.headers);
        out.writeVLong(this.version);
        out.writeVLong(this.modifiedDate);
        out.writeOptionalWriteable(this.lastSuccess);
        out.writeOptionalWriteable(this.lastFailure);
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
            .setLastSuccess(metadata.getLastSuccess())
            .setLastFailure(metadata.getLastFailure());
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

    public SnapshotInvocationRecord getLastSuccess() {
        return lastSuccess;
    }

    public SnapshotInvocationRecord getLastFailure() {
        return lastFailure;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(POLICY.getPreferredName(), policy);
        builder.field(HEADERS.getPreferredName(), headers);
        builder.field(VERSION.getPreferredName(), version);
        builder.timeField(MODIFIED_DATE_MILLIS.getPreferredName(), MODIFIED_DATE.getPreferredName(), modifiedDate);
        if (Objects.nonNull(lastSuccess)) {
            builder.field(LAST_SUCCESS.getPreferredName(), lastSuccess);
        }
        if (Objects.nonNull(lastFailure)) {
            builder.field(LAST_FAILURE.getPreferredName(), lastFailure);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(policy, headers, version, modifiedDate, lastSuccess, lastFailure);
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
            Objects.equals(lastSuccess, other.lastSuccess) &&
            Objects.equals(lastFailure, other.lastFailure);
    }

    @Override
    public String toString() {
        // Note: this is on purpose. While usually we would use Strings.toString(this) to render
        // this using toXContent, it may contain sensitive information in the headers and thus
        // should not emit them in case it accidentally gets logged.
        return super.toString();
    }

    public static class Builder {

        private Builder() {
        }

        private SnapshotLifecyclePolicy policy;
        private Map<String, String> headers;
        private long version = 1L;
        private Long modifiedDate;
        private SnapshotInvocationRecord lastSuccessDate;
        private SnapshotInvocationRecord lastFailureDate;

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

        public Builder setLastSuccess(SnapshotInvocationRecord lastSuccessDate) {
            this.lastSuccessDate = lastSuccessDate;
            return this;
        }

        public Builder setLastFailure(SnapshotInvocationRecord lastFailureDate) {
            this.lastFailureDate = lastFailureDate;
            return this;
        }

        public SnapshotLifecyclePolicyMetadata build() {
            return new SnapshotLifecyclePolicyMetadata(
                Objects.requireNonNull(policy),
                Optional.ofNullable(headers).orElse(new HashMap<>()),
                version,
                Objects.requireNonNull(modifiedDate, "modifiedDate must be set"),
                lastSuccessDate,
                lastFailureDate);
        }
    }

}
