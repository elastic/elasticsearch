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

    public LogsDBFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        indicesCount = input.readVInt();
        indicesWithSyntheticSource = input.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(indicesCount);
        out.writeVInt(indicesWithSyntheticSource);
    }

    public LogsDBFeatureSetUsage(boolean available, boolean enabled, int indicesCount, int indicesWithSyntheticSource) {
        super(XPackField.LOGSDB, available, enabled);
        this.indicesCount = indicesCount;
        this.indicesWithSyntheticSource = indicesWithSyntheticSource;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.LOGSDB_TELEMETRY;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field("indices_count", indicesCount);
        builder.field("indices_with_synthetic_source", indicesWithSyntheticSource);
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, enabled, indicesCount, indicesWithSyntheticSource);
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
            && Objects.equals(indicesWithSyntheticSource, other.indicesWithSyntheticSource);
    }
}
