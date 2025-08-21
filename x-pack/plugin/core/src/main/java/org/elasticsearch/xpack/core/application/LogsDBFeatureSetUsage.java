/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.application;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Objects;

public final class LogsDBFeatureSetUsage extends XPackFeatureUsage {
    private final int indicesCount;
    private final int indicesWithSyntheticSource;
    private final long numDocs;
    private final long sizeInBytes;
    private final boolean hasCustomCutoffDate;

    public LogsDBFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        indicesCount = input.readVInt();
        indicesWithSyntheticSource = input.readVInt();
        if (input.getTransportVersion().onOrAfter(TransportVersions.V_8_17_0)) {
            numDocs = input.readVLong();
            sizeInBytes = input.readVLong();
        } else {
            numDocs = 0;
            sizeInBytes = 0;
        }
        var transportVersion = input.getTransportVersion();
        if (transportVersion.isPatchFrom(TransportVersions.V_8_17_0)
            || transportVersion.onOrAfter(TransportVersions.LOGSDB_TELEMETRY_CUSTOM_CUTOFF_DATE)) {
            hasCustomCutoffDate = input.readBoolean();
        } else {
            hasCustomCutoffDate = false;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(indicesCount);
        out.writeVInt(indicesWithSyntheticSource);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_17_0)) {
            out.writeVLong(numDocs);
            out.writeVLong(sizeInBytes);
        }
        var transportVersion = out.getTransportVersion();
        if (transportVersion.isPatchFrom(TransportVersions.V_8_17_0)
            || transportVersion.onOrAfter(TransportVersions.LOGSDB_TELEMETRY_CUSTOM_CUTOFF_DATE)) {
            out.writeBoolean(hasCustomCutoffDate);
        }
    }

    public LogsDBFeatureSetUsage(
        boolean available,
        boolean enabled,
        int indicesCount,
        int indicesWithSyntheticSource,
        long numDocs,
        long sizeInBytes,
        boolean hasCustomCutoffDate
    ) {
        super(XPackField.LOGSDB, available, enabled);
        this.indicesCount = indicesCount;
        this.indicesWithSyntheticSource = indicesWithSyntheticSource;
        this.numDocs = numDocs;
        this.sizeInBytes = sizeInBytes;
        this.hasCustomCutoffDate = hasCustomCutoffDate;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_17_0;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field("indices_count", indicesCount);
        builder.field("indices_with_synthetic_source", indicesWithSyntheticSource);
        builder.field("num_docs", numDocs);
        builder.field("size_in_bytes", sizeInBytes);
        builder.field("has_custom_cutoff_date", hasCustomCutoffDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, enabled, indicesCount, indicesWithSyntheticSource, numDocs, sizeInBytes, hasCustomCutoffDate);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        LogsDBFeatureSetUsage other = (LogsDBFeatureSetUsage) obj;
        return Objects.equals(available, other.available)
            && Objects.equals(enabled, other.enabled)
            && Objects.equals(indicesCount, other.indicesCount)
            && Objects.equals(indicesWithSyntheticSource, other.indicesWithSyntheticSource)
            && Objects.equals(numDocs, other.numDocs)
            && Objects.equals(sizeInBytes, other.sizeInBytes)
            && Objects.equals(hasCustomCutoffDate, other.hasCustomCutoffDate);
    }
}
