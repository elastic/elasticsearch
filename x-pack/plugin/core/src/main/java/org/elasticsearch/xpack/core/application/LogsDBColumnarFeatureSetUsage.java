/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.application;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Objects;

public final class LogsDBColumnarFeatureSetUsage extends XPackFeatureUsage {
    public static final TransportVersion LOGSDB_COLUMNAR_USAGE = TransportVersion.fromName("logsdb_columnar_usage");

    private final int indicesCount;
    private final int indicesWithSyntheticSource;
    private final long numDocs;
    private final long sizeInBytes;
    private final int dataStreamsCount;
    private final int dataStreamsManagedByIlm;
    private final int dataStreamsManagedByDlm;

    public LogsDBColumnarFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        indicesCount = input.readVInt();
        indicesWithSyntheticSource = input.readVInt();
        numDocs = input.readVLong();
        sizeInBytes = input.readVLong();
        dataStreamsCount = input.readVInt();
        dataStreamsManagedByIlm = input.readVInt();
        dataStreamsManagedByDlm = input.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(indicesCount);
        out.writeVInt(indicesWithSyntheticSource);
        out.writeVLong(numDocs);
        out.writeVLong(sizeInBytes);
        out.writeVInt(dataStreamsCount);
        out.writeVInt(dataStreamsManagedByIlm);
        out.writeVInt(dataStreamsManagedByDlm);
    }

    public LogsDBColumnarFeatureSetUsage(
        boolean available,
        boolean enabled,
        int indicesCount,
        int indicesWithSyntheticSource,
        long numDocs,
        long sizeInBytes,
        int dataStreamsCount,
        int dataStreamsManagedByIlm,
        int dataStreamsManagedByDlm
    ) {
        super(XPackField.LOGSDB_COLUMNAR, available, enabled);
        this.indicesCount = indicesCount;
        this.indicesWithSyntheticSource = indicesWithSyntheticSource;
        this.numDocs = numDocs;
        this.sizeInBytes = sizeInBytes;
        this.dataStreamsCount = dataStreamsCount;
        this.dataStreamsManagedByIlm = dataStreamsManagedByIlm;
        this.dataStreamsManagedByDlm = dataStreamsManagedByDlm;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return LOGSDB_COLUMNAR_USAGE;
    }

    public int indicesCount() {
        return indicesCount;
    }

    public int indicesWithSyntheticSource() {
        return indicesWithSyntheticSource;
    }

    public long numDocs() {
        return numDocs;
    }

    public long sizeInBytes() {
        return sizeInBytes;
    }

    public int dataStreamsCount() {
        return dataStreamsCount;
    }

    public int dataStreamsManagedByIlm() {
        return dataStreamsManagedByIlm;
    }

    public int dataStreamsManagedByDlm() {
        return dataStreamsManagedByDlm;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field("indices_count", indicesCount);
        builder.field("indices_with_synthetic_source", indicesWithSyntheticSource);
        builder.field("num_docs", numDocs);
        builder.field("size_in_bytes", sizeInBytes);
        builder.field("data_streams_count", dataStreamsCount);
        builder.field("data_streams_managed_by_ilm", dataStreamsManagedByIlm);
        builder.field("data_streams_managed_by_dlm", dataStreamsManagedByDlm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            available,
            enabled,
            indicesCount,
            indicesWithSyntheticSource,
            numDocs,
            sizeInBytes,
            dataStreamsCount,
            dataStreamsManagedByIlm,
            dataStreamsManagedByDlm
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        LogsDBColumnarFeatureSetUsage other = (LogsDBColumnarFeatureSetUsage) obj;
        return Objects.equals(available, other.available)
            && Objects.equals(enabled, other.enabled)
            && Objects.equals(indicesCount, other.indicesCount)
            && Objects.equals(indicesWithSyntheticSource, other.indicesWithSyntheticSource)
            && Objects.equals(numDocs, other.numDocs)
            && Objects.equals(sizeInBytes, other.sizeInBytes)
            && Objects.equals(dataStreamsCount, other.dataStreamsCount)
            && Objects.equals(dataStreamsManagedByIlm, other.dataStreamsManagedByIlm)
            && Objects.equals(dataStreamsManagedByDlm, other.dataStreamsManagedByDlm);
    }
}
