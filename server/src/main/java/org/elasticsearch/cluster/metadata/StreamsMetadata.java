/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Objects;

/**
 * Metadata for the Streams feature, which allows enabling or disabling logs for data streams.
 * This class implements the Metadata.ProjectCustom interface to allow it to be stored in the cluster state.
 */
public class StreamsMetadata extends AbstractNamedDiffable<Metadata.ProjectCustom> implements Metadata.ProjectCustom {

    public static final String TYPE = "streams";
    public static final StreamsMetadata EMPTY = new StreamsMetadata(false, false, false);
    private static final ParseField LOGS_ENABLED = new ParseField("logs_enabled");
    private static final ParseField LOGS_ECS_ENABLED = new ParseField("logs_ecs_enabled");
    private static final ParseField LOGS_OTEL_ENABLED = new ParseField("logs_otel_enabled");
    private static final ConstructingObjectParser<StreamsMetadata, Void> PARSER = new ConstructingObjectParser<>(TYPE, false, args -> {
        boolean logsEnabled = args[0] != null && (boolean) args[0];
        boolean logsECSEnabled = args[1] != null && (boolean) args[1];
        boolean logsOtelEnabled = args[2] != null && (boolean) args[2];
        return new StreamsMetadata(logsEnabled, logsECSEnabled, logsOtelEnabled);
    });
    static {
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), LOGS_ENABLED);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), LOGS_ECS_ENABLED);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), LOGS_OTEL_ENABLED);
    }
    private static final TransportVersion STREAMS_LOGS_SUPPORT = TransportVersion.fromName("streams_logs_support");
    public static TransportVersion STREAM_SPLIT_VERSION = TransportVersion.fromName("stream_split_version");

    public boolean logsEnabled;
    public boolean logsECSEnabled;
    public boolean logsOTelEnabled;

    public StreamsMetadata(StreamInput in) throws IOException {
        logsEnabled = in.readBoolean();
        if (in.getTransportVersion().supports(STREAM_SPLIT_VERSION)) {
            logsECSEnabled = in.readBoolean();
            logsOTelEnabled = in.readBoolean();
        }
    }

    public StreamsMetadata(boolean logsEnabled, boolean logsECSEnabled, boolean logsOTelEnabled) {
        this.logsEnabled = logsEnabled;
        this.logsECSEnabled = logsECSEnabled;
        this.logsOTelEnabled = logsOTelEnabled;
    }

    public StreamsMetadata toggleLogs(boolean logsEnabled) {
        return new StreamsMetadata(logsEnabled, this.logsECSEnabled, this.logsOTelEnabled);
    }

    public StreamsMetadata toggleECS(boolean ecsEnabled) {
        return new StreamsMetadata(this.logsEnabled, ecsEnabled, this.logsOTelEnabled);
    }

    public StreamsMetadata toggleOTel(boolean oTelEnabled) {
        return new StreamsMetadata(this.logsEnabled, this.logsECSEnabled, oTelEnabled);
    }

    public boolean isLogsEnabled() {
        return logsEnabled;
    }

    public boolean isLogsECSEnabled() {
        return logsECSEnabled;
    }

    public boolean isLogsOTelEnabled() {
        return logsOTelEnabled;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return Metadata.ALL_CONTEXTS;
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return STREAMS_LOGS_SUPPORT;
    }

    @Override
    public boolean supportsVersion(TransportVersion version) {
        return version.supports(STREAMS_LOGS_SUPPORT);
    }

    public static NamedDiff<Metadata.ProjectCustom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.ProjectCustom.class, TYPE, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(logsEnabled);
        if (out.getTransportVersion().supports(STREAM_SPLIT_VERSION)) {
            out.writeBoolean(logsECSEnabled);
            out.writeBoolean(logsOTelEnabled);
        }
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(
            ChunkedToXContentHelper.chunk((builder, bParams) -> builder.field(LOGS_ENABLED.getPreferredName(), logsEnabled)),
            ChunkedToXContentHelper.chunk((builder, bParams) -> builder.field(LOGS_ECS_ENABLED.getPreferredName(), logsECSEnabled)),
            ChunkedToXContentHelper.chunk((builder, bParams) -> builder.field(LOGS_OTEL_ENABLED.getPreferredName(), logsOTelEnabled))
        );
    }

    @Override
    public boolean equals(Object o) {
        if ((o instanceof StreamsMetadata that)) {
            return logsEnabled == that.logsEnabled && logsECSEnabled == that.logsECSEnabled && logsOTelEnabled == that.logsOTelEnabled;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(logsEnabled, logsECSEnabled, logsOTelEnabled);
    }

    public static StreamsMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
