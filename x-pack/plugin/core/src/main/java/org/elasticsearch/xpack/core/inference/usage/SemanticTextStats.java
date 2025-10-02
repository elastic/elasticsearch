/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.usage;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class SemanticTextStats implements ToXContentObject, Writeable {

    private static final String FIELD_COUNT = "field_count";
    private static final String INDICES_COUNT = "indices_count";
    private static final String INFERENCE_ID_COUNT = "inference_id_count";

    private long fieldCount;
    private long indicesCount;
    private long inferenceIdCount;

    public SemanticTextStats() {}

    public SemanticTextStats(long fieldCount, long indicesCount, long inferenceIdCount) {
        this.fieldCount = fieldCount;
        this.indicesCount = indicesCount;
        this.inferenceIdCount = inferenceIdCount;
    }

    public SemanticTextStats(StreamInput in) throws IOException {
        fieldCount = in.readVLong();
        indicesCount = in.readVLong();
        inferenceIdCount = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(fieldCount);
        out.writeVLong(indicesCount);
        out.writeVLong(inferenceIdCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD_COUNT, fieldCount);
        builder.field(INDICES_COUNT, indicesCount);
        builder.field(INFERENCE_ID_COUNT, inferenceIdCount);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SemanticTextStats that = (SemanticTextStats) o;
        return fieldCount == that.fieldCount && indicesCount == that.indicesCount && inferenceIdCount == that.inferenceIdCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldCount, indicesCount, inferenceIdCount);
    }

    public long getFieldCount() {
        return fieldCount;
    }

    public long getIndicesCount() {
        return indicesCount;
    }

    public long getInferenceIdCount() {
        return inferenceIdCount;
    }

    public void addFieldCount(long fieldCount) {
        this.fieldCount += fieldCount;
    }

    public void incIndicesCount() {
        this.indicesCount++;
    }

    public void setInferenceIdCount(long inferenceIdCount) {
        this.inferenceIdCount = inferenceIdCount;
    }

    public boolean isEmpty() {
        return fieldCount == 0 && indicesCount == 0 && inferenceIdCount == 0;
    }
}
