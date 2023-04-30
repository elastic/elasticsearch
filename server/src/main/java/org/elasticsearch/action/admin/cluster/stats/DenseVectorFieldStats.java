/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.apache.lucene.index.VectorValues.MAX_DIMENSIONS;

/**
 * Holds enhanced stats about a dense vector mapped field.
 */
public final class DenseVectorFieldStats extends FieldStats {
    int indexedVectorCount; // number of times vectors with index:true are used in mappings of this cluster
    int indexedVectorDimMin; // minimum dimension of indexed vectors in this cluster
    int indexedVectorDimMax; // maximum dimension of indexed vectors in this cluster

    DenseVectorFieldStats(String name) {
        super(name);
        indexedVectorCount = 0;
        indexedVectorDimMin = MAX_DIMENSIONS;
        indexedVectorDimMax = 0;
    }

    DenseVectorFieldStats(StreamInput in) throws IOException {
        super(in);
        indexedVectorCount = in.readVInt();
        indexedVectorDimMin = in.readVInt();
        indexedVectorDimMax = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(indexedVectorCount);
        out.writeVInt(indexedVectorDimMin);
        out.writeVInt(indexedVectorDimMax);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("indexed_vector_count", indexedVectorCount);
        builder.field("indexed_vector_dim_min", indexedVectorDimMin);
        builder.field("indexed_vector_dim_max", indexedVectorDimMax);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (super.equals(o) == false) {
            return false;
        }
        DenseVectorFieldStats that = (DenseVectorFieldStats) o;
        return indexedVectorCount == that.indexedVectorCount
            && indexedVectorDimMin == that.indexedVectorDimMin
            && indexedVectorDimMax == that.indexedVectorDimMax;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), indexedVectorCount, indexedVectorDimMin, indexedVectorDimMax);
    }
}
