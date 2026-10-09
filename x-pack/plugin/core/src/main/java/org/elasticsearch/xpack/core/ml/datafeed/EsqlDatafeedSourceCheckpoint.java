/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class EsqlDatafeedSourceCheckpoint implements ToXContentObject, Writeable {

    public static final ParseField JOB_ID = new ParseField("job_id");
    public static final ParseField DATAFEED_ID = new ParseField("datafeed_id");
    public static final ParseField SOURCE_END_MS = new ParseField("source_end_ms");
    public static final ParseField FINGERPRINT = new ParseField("fingerprint");

    public static final ParseField TYPE = new ParseField("esql_datafeed_source_checkpoint");

    public static final ConstructingObjectParser<EsqlDatafeedSourceCheckpoint, Void> PARSER = createParser();

    private static ConstructingObjectParser<EsqlDatafeedSourceCheckpoint, Void> createParser() {
        ConstructingObjectParser<EsqlDatafeedSourceCheckpoint, Void> parser = new ConstructingObjectParser<>(
            TYPE.getPreferredName(),
            true,
            args -> {
                String jobId = (String) args[0];
                String datafeedId = (String) args[1];
                Long sourceEndMs = (Long) args[2];
                String fingerprint = (String) args[3];
                return new EsqlDatafeedSourceCheckpoint(jobId, datafeedId, sourceEndMs, fingerprint);
            }
        );
        parser.declareString(constructorArg(), JOB_ID);
        parser.declareString(constructorArg(), DATAFEED_ID);
        parser.declareLong(constructorArg(), SOURCE_END_MS);
        parser.declareString(constructorArg(), FINGERPRINT);
        return parser;
    }

    public static String documentId(String jobId) {
        return jobId + "_esql_source_checkpoint";
    }

    public static String computeFingerprint(String esqlQuery, String sourceTimeField, String emittedTimeField, TimeValue groupingInterval) {
        String material = esqlQuery + '\0' + sourceTimeField + '\0' + emittedTimeField + '\0' + groupingInterval.getStringRep();
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(material.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest not available", e);
        }
    }

    public static String computeFingerprint(DatafeedConfig datafeedConfig, String emittedTimeField) {
        return computeFingerprint(
            datafeedConfig.getEsqlQuery(),
            datafeedConfig.getSourceTimeField(),
            emittedTimeField,
            datafeedConfig.getGroupingInterval()
        );
    }

    private final String jobId;
    private final String datafeedId;
    private final long sourceEndMs;
    private final String fingerprint;

    public EsqlDatafeedSourceCheckpoint(String jobId, String datafeedId, long sourceEndMs, String fingerprint) {
        this.jobId = Objects.requireNonNull(jobId);
        this.datafeedId = Objects.requireNonNull(datafeedId);
        this.sourceEndMs = sourceEndMs;
        this.fingerprint = Objects.requireNonNull(fingerprint);
    }

    public EsqlDatafeedSourceCheckpoint(StreamInput in) throws IOException {
        this.jobId = in.readString();
        this.datafeedId = in.readString();
        this.sourceEndMs = in.readLong();
        this.fingerprint = in.readString();
    }

    public String getJobId() {
        return jobId;
    }

    public String getDatafeedId() {
        return datafeedId;
    }

    public long getSourceEndMs() {
        return sourceEndMs;
    }

    public String getFingerprint() {
        return fingerprint;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(jobId);
        out.writeString(datafeedId);
        out.writeLong(sourceEndMs);
        out.writeString(fingerprint);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        if (params.paramAsBoolean(ToXContentParams.FOR_INTERNAL_STORAGE, false)) {
            builder.field(Result.RESULT_TYPE.getPreferredName(), TYPE.getPreferredName());
        }
        builder.field(JOB_ID.getPreferredName(), jobId);
        builder.field(DATAFEED_ID.getPreferredName(), datafeedId);
        builder.field(SOURCE_END_MS.getPreferredName(), sourceEndMs);
        builder.field(FINGERPRINT.getPreferredName(), fingerprint);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        EsqlDatafeedSourceCheckpoint other = (EsqlDatafeedSourceCheckpoint) obj;
        return sourceEndMs == other.sourceEndMs
            && Objects.equals(jobId, other.jobId)
            && Objects.equals(datafeedId, other.datafeedId)
            && Objects.equals(fingerprint, other.fingerprint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, datafeedId, sourceEndMs, fingerprint);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Nullable
    public static EsqlDatafeedSourceCheckpoint validateFingerprint(
        @Nullable EsqlDatafeedSourceCheckpoint checkpoint,
        String expectedFingerprint
    ) {
        if (checkpoint == null) {
            return null;
        }
        if (checkpoint.getFingerprint().equals(expectedFingerprint)) {
            return checkpoint;
        }
        return null;
    }
}
