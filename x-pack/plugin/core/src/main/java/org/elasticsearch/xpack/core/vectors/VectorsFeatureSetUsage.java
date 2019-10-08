/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.vectors;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Objects;

public class VectorsFeatureSetUsage extends XPackFeatureSet.Usage {

    private final int numDenseVectorFields;
    private final int numSparseVectorFields;
    private final int avgDenseVectorDims;

    public VectorsFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
        numDenseVectorFields = input.readVInt();
        numSparseVectorFields = input.readVInt();
        avgDenseVectorDims = input.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(numDenseVectorFields);
        out.writeVInt(numSparseVectorFields);
        out.writeVInt(avgDenseVectorDims);
    }

    public VectorsFeatureSetUsage(boolean available, boolean enabled, int numDenseVectorFields, int numSparseVectorFields,
            int avgDenseVectorDims) {
        super(XPackField.VECTORS, available, enabled);
        this.numDenseVectorFields = numDenseVectorFields;
        this.numSparseVectorFields = numSparseVectorFields;
        this.avgDenseVectorDims = avgDenseVectorDims;
    }


    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field("dense_vector_fields_count", numDenseVectorFields);
        builder.field("sparse_vector_fields_count", numSparseVectorFields);
        builder.field("dense_vector_dims_avg_count", avgDenseVectorDims);
    }

    public int numDenseVectorFields() {
        return numDenseVectorFields;
    }
    public int numSparseVectorFields() {
        return numSparseVectorFields;
    }
    public int avgDenseVectorDims() {
        return avgDenseVectorDims;
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, enabled, numDenseVectorFields, numSparseVectorFields, avgDenseVectorDims);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof VectorsFeatureSetUsage == false) return false;
        VectorsFeatureSetUsage other = (VectorsFeatureSetUsage) obj;
        return available == other.available && enabled == other.enabled && numDenseVectorFields == other.numDenseVectorFields
            && numSparseVectorFields == other.numSparseVectorFields && avgDenseVectorDims == other.avgDenseVectorDims;
    }
}
