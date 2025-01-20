/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.TransformConfigVersion;
import org.elasticsearch.xpack.core.transform.TransformField;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

public class TransformTaskParams implements SimpleDiffable<TransformTaskParams>, PersistentTaskParams {

    public static final String NAME = TransformField.TASK_NAME;
    public static final ParseField FROM = TransformField.FROM;
    public static final ParseField FREQUENCY = TransformField.FREQUENCY;
    public static final ParseField REQUIRES_REMOTE = new ParseField("requires_remote");

    private final String transformId;
    private final TransformConfigVersion version;
    private final Instant from;
    private final TimeValue frequency;
    private final Boolean requiresRemote;

    public static final ConstructingObjectParser<TransformTaskParams, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        true,
        a -> new TransformTaskParams((String) a[0], (String) a[1], (Long) a[2], (String) a[3], (Boolean) a[4])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TransformField.ID);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TransformField.VERSION);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), FROM);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), FREQUENCY);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), REQUIRES_REMOTE);
    }

    private TransformTaskParams(String transformId, String version, Long from, String frequency, Boolean remote) {
        this(
            transformId,
            version == null ? null : TransformConfigVersion.fromString(version),
            from == null ? null : Instant.ofEpochMilli(from),
            frequency == null ? null : TimeValue.parseTimeValue(frequency, FREQUENCY.getPreferredName()),
            remote == null ? false : remote.booleanValue()
        );
    }

    public TransformTaskParams(String transformId, TransformConfigVersion version, TimeValue frequency, boolean remote) {
        this(transformId, version, null, frequency, remote);
    }

    public TransformTaskParams(String transformId, TransformConfigVersion version, Instant from, TimeValue frequency, boolean remote) {
        this.transformId = transformId;
        this.version = version == null ? TransformConfigVersion.V_7_2_0 : version;
        this.from = from;
        this.frequency = frequency;
        this.requiresRemote = remote;
    }

    public TransformTaskParams(StreamInput in) throws IOException {
        this.transformId = in.readString();
        this.version = TransformConfigVersion.readVersion(in);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_7_0)) {
            this.from = in.readOptionalInstant();
        } else {
            this.from = null;
        }
        this.frequency = in.readOptionalTimeValue();
        this.requiresRemote = in.readBoolean();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_7_17_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(transformId);
        TransformConfigVersion.writeVersion(version, out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_7_0)) {
            out.writeOptionalInstant(from);
        }
        out.writeOptionalTimeValue(frequency);
        out.writeBoolean(requiresRemote);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TransformField.ID.getPreferredName(), transformId);
        builder.field(TransformField.VERSION.getPreferredName(), version);
        if (from != null) {
            builder.field(FROM.getPreferredName(), from.toEpochMilli());
        }
        if (frequency != null) {
            builder.field(FREQUENCY.getPreferredName(), frequency.getStringRep());
        }
        builder.field(REQUIRES_REMOTE.getPreferredName(), requiresRemote);
        builder.endObject();
        return builder;
    }

    public String getId() {
        return transformId;
    }

    public TransformConfigVersion getVersion() {
        return version;
    }

    public Instant from() {
        return from;
    }

    public TimeValue getFrequency() {
        return frequency;
    }

    public boolean requiresRemote() {
        return requiresRemote;
    }

    public static TransformTaskParams fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        TransformTaskParams that = (TransformTaskParams) other;

        return Objects.equals(this.transformId, that.transformId)
            && Objects.equals(this.version, that.version)
            && Objects.equals(this.from, that.from)
            && Objects.equals(this.frequency, that.frequency)
            && this.requiresRemote == that.requiresRemote;
    }

    @Override
    public int hashCode() {
        return Objects.hash(transformId, version, from, frequency, requiresRemote);
    }
}
