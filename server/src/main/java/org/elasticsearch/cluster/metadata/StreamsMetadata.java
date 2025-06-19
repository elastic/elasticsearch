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
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Objects;

/**
 * Metadata for the Streams feature, which allows enabling or disabling logs for data streams.
 * This class implements the Metadata.ProjectCustom interface to allow it to be stored in the cluster state.
 */
public class StreamsMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {

    public static final String TYPE = "streams";
    public static final StreamsMetadata EMPTY = new StreamsMetadata(false);

    public boolean logsEnabled;

    public StreamsMetadata(StreamInput in) throws IOException {
        logsEnabled = in.readBoolean();
    }

    public StreamsMetadata(boolean logsEnabled) {
        this.logsEnabled = logsEnabled;
    }

    public boolean isLogsEnabled() {
        return logsEnabled;
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
        return TransportVersions.STREAMS_LOGS_SUPPORT_8_19;
    }

    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Metadata.Custom.class, TYPE, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(logsEnabled);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(ChunkedToXContentHelper.singleChunk((builder, bParams) -> builder.field("logs_enabled", logsEnabled)));
    }

    @Override
    public boolean equals(Object o) {
        if ((o instanceof StreamsMetadata that)) {
            return logsEnabled == that.logsEnabled;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(logsEnabled);
    }
}
