/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.slm;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ClientHelper.assertNoAuthorizationHeader;

/**
 * {@code SnapshotLifecyclePolicyMetadata} encapsulates a {@link SnapshotLifecyclePolicy} as well as
 * the additional meta information link headers used for execution, version (a monotonically
 * incrementing number), and last modified date
 */
public class SnapshotLifecyclePolicyMetadata implements SimpleDiffable<SnapshotLifecyclePolicyMetadata>, ToXContentObject {

    static final ParseField POLICY = new ParseField("policy");
    static final ParseField HEADERS = new ParseField("headers");
    static final ParseField VERSION = new ParseField("version");
    static final ParseField MODIFIED_DATE_MILLIS = new ParseField("modified_date_millis");
    static final ParseField MODIFIED_DATE = new ParseField("modified_date");
    static final ParseField LAST_SUCCESS = new ParseField("last_success");
    static final ParseField LAST_FAILURE = new ParseField("last_failure");
    static final ParseField INVOCATIONS_SINCE_LAST_SUCCESS = new ParseField("invocations_since_last_success");
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
    private final long invocationsSinceLastSuccess;

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<SnapshotLifecyclePolicyMetadata, String> PARSER = new ConstructingObjectParser<>(
        "snapshot_policy_metadata",
        a -> {
            SnapshotLifecyclePolicy policy = (SnapshotLifecyclePolicy) a[0];
            SnapshotInvocationRecord lastSuccess = (SnapshotInvocationRecord) a[4];
            SnapshotInvocationRecord lastFailure = (SnapshotInvocationRecord) a[5];

            return builder().setPolicy(policy)
                .setHeaders((Map<String, String>) a[1])
                .setVersion((long) a[2])
                .setModifiedDate((long) a[3])
                .setLastSuccess(lastSuccess)
                .setLastFailure(lastFailure)
                .setInvocationsSinceLastSuccess(a[6] == null ? 0L : ((long) a[6]))
                .build();
        }
    );

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), SnapshotLifecyclePolicy::parse, POLICY);
        PARSER.declareField(ConstructingObjectParser.constructorArg(), XContentParser::mapStrings, HEADERS, ObjectParser.ValueType.OBJECT);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), VERSION);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), MODIFIED_DATE_MILLIS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), SnapshotInvocationRecord::parse, LAST_SUCCESS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), SnapshotInvocationRecord::parse, LAST_FAILURE);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), INVOCATIONS_SINCE_LAST_SUCCESS);
    }

    public static SnapshotLifecyclePolicyMetadata parse(XContentParser parser, String name) {
        return PARSER.apply(parser, name);
    }

    public SnapshotLifecyclePolicyMetadata(
        SnapshotLifecyclePolicy policy,
        Map<String, String> headers,
        long version,
        long modifiedDate,
        SnapshotInvocationRecord lastSuccess,
        SnapshotInvocationRecord lastFailure,
        long invocationsSinceLastSuccess
    ) {
        this.policy = policy;
        this.headers = headers;
        assertNoAuthorizationHeader(this.headers);
        this.version = version;
        this.modifiedDate = modifiedDate;
        this.lastSuccess = lastSuccess;
        this.lastFailure = lastFailure;
        this.invocationsSinceLastSuccess = invocationsSinceLastSuccess;
    }

    @SuppressWarnings("unchecked")
    SnapshotLifecyclePolicyMetadata(StreamInput in) throws IOException {
        this.policy = new SnapshotLifecyclePolicy(in);
        this.headers = (Map<String, String>) in.readGenericValue();
        assertNoAuthorizationHeader(this.headers);
        this.version = in.readVLong();
        this.modifiedDate = in.readVLong();
        this.lastSuccess = in.readOptionalWriteable(SnapshotInvocationRecord::new);
        this.lastFailure = in.readOptionalWriteable(SnapshotInvocationRecord::new);
        this.invocationsSinceLastSuccess = in.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0) ? in.readVLong() : 0L;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.policy.writeTo(out);
        out.writeGenericValue(this.headers);
        out.writeVLong(this.version);
        out.writeVLong(this.modifiedDate);
        out.writeOptionalWriteable(this.lastSuccess);
        out.writeOptionalWriteable(this.lastFailure);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
            out.writeVLong(this.invocationsSinceLastSuccess);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(SnapshotLifecyclePolicyMetadata metadata) {
        if (metadata == null) {
            return builder();
        }
        return new Builder().setHeaders(metadata.getHeaders())
            .setPolicy(metadata.getPolicy())
            .setVersion(metadata.getVersion())
            .setModifiedDate(metadata.getModifiedDate())
            .setLastSuccess(metadata.getLastSuccess())
            .setLastFailure(metadata.getLastFailure())
            .setInvocationsSinceLastSuccess(metadata.getInvocationsSinceLastSuccess());
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public SnapshotLifecyclePolicy getPolicy() {
        return policy;
    }

    public String getId() {
        return policy.getId();
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

    public long getInvocationsSinceLastSuccess() {
        return invocationsSinceLastSuccess;
    }

    public SchedulerEngine.Job buildSchedulerJob(String jobId) {
        return policy.buildSchedulerJob(jobId, modifiedDate);
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
        builder.field(INVOCATIONS_SINCE_LAST_SUCCESS.getPreferredName(), invocationsSinceLastSuccess);
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(policy, headers, version, modifiedDate, lastSuccess, lastFailure, invocationsSinceLastSuccess);
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
        return Objects.equals(policy, other.policy)
            && Objects.equals(headers, other.headers)
            && Objects.equals(version, other.version)
            && Objects.equals(modifiedDate, other.modifiedDate)
            && Objects.equals(lastSuccess, other.lastSuccess)
            && Objects.equals(lastFailure, other.lastFailure)
            && Objects.equals(invocationsSinceLastSuccess, other.invocationsSinceLastSuccess);
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
        private SnapshotInvocationRecord lastSuccessDate;
        private SnapshotInvocationRecord lastFailureDate;
        private long invocationsSinceLastSuccess = 0L;

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

        public Builder setLastSuccess(SnapshotInvocationRecord lastSuccess) {
            this.lastSuccessDate = lastSuccess;
            return this;
        }

        public Builder setLastFailure(SnapshotInvocationRecord lastFailure) {
            this.lastFailureDate = lastFailure;
            return this;
        }

        public Builder setInvocationsSinceLastSuccess(long invocationsSinceLastSuccess) {
            this.invocationsSinceLastSuccess = invocationsSinceLastSuccess;
            return this;
        }

        public Builder incrementInvocationsSinceLastSuccess() {
            this.invocationsSinceLastSuccess++;
            return this;
        }

        public SnapshotLifecyclePolicyMetadata build() {
            return new SnapshotLifecyclePolicyMetadata(
                Objects.requireNonNull(policy),
                Optional.ofNullable(headers).orElse(new HashMap<>()),
                version,
                Objects.requireNonNull(modifiedDate, "modifiedDate must be set"),
                lastSuccessDate,
                lastFailureDate,
                invocationsSinceLastSuccess
            );
        }
    }

}
