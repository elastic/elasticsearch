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

/**
 * The {@code vectordb_columnar} section of the {@code _xpack/usage} response: how many indices use that index mode and
 * how many documents they hold. Values are produced by
 * {@link org.elasticsearch.xpack.core.action.VectorDBColumnarUsageTransportAction VectorDBColumnarUsageTransportAction}.
 */
public final class VectorDBColumnarFeatureSetUsage extends XPackFeatureUsage {
    public static final TransportVersion VECTORDB_COLUMNAR_USAGE = TransportVersion.fromName("vectordb_columnar_usage");

    private final int indicesCount;
    private final long numDocs;

    public VectorDBColumnarFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        indicesCount = input.readVInt();
        numDocs = input.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(indicesCount);
        out.writeVLong(numDocs);
    }

    public VectorDBColumnarFeatureSetUsage(boolean available, boolean enabled, int indicesCount, long numDocs) {
        super(XPackField.VECTORDB_COLUMNAR, available, enabled);
        this.indicesCount = Math.max(0, indicesCount);
        this.numDocs = Math.max(0L, numDocs);
    }

    public int indicesCount() {
        return indicesCount;
    }

    public long numDocs() {
        return numDocs;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return VECTORDB_COLUMNAR_USAGE;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field("indices_count", indicesCount);
        builder.field("num_docs", numDocs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, enabled, indicesCount, numDocs);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        VectorDBColumnarFeatureSetUsage other = (VectorDBColumnarFeatureSetUsage) obj;
        return Objects.equals(available, other.available)
            && Objects.equals(enabled, other.enabled)
            && Objects.equals(indicesCount, other.indicesCount)
            && Objects.equals(numDocs, other.numDocs);
    }
}
