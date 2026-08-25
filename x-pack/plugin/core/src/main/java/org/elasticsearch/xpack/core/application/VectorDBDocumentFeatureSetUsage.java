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

public final class VectorDBDocumentFeatureSetUsage extends XPackFeatureUsage {
    public static final TransportVersion VECTORDB_DOCUMENT_USAGE = TransportVersion.fromName("vectordb_document_usage");

    private final int indicesCount;
    private final long numDocs;

    public VectorDBDocumentFeatureSetUsage(StreamInput input) throws IOException {
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

    public VectorDBDocumentFeatureSetUsage(boolean available, boolean enabled, int indicesCount, long numDocs) {
        super(XPackField.VECTORDB_DOCUMENT, available, enabled);
        this.indicesCount = indicesCount;
        this.numDocs = numDocs;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return VECTORDB_DOCUMENT_USAGE;
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
        VectorDBDocumentFeatureSetUsage other = (VectorDBDocumentFeatureSetUsage) obj;
        return Objects.equals(available, other.available)
            && Objects.equals(enabled, other.enabled)
            && Objects.equals(indicesCount, other.indicesCount)
            && Objects.equals(numDocs, other.numDocs);
    }
}
