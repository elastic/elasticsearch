/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.stats;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Holds enhanced stats about a dense vector mapped field.
 */
public final class DenseVectorFieldStats extends FieldStats {
    static final int UNSET = -1;
    static final String NOT_INDEXED = "not_indexed";
    Map<String, Integer> vectorIndexTypeCount; // count of mappings by index type
    Map<String, Integer> vectorSimilarityTypeCount; // count of mappings by similarity
    Map<String, Integer> vectorElementTypeCount; // count of mappings by element type
    int indexedVectorCount; // number of times vectors with index:true are used in mappings of this cluster
    int indexedVectorDimMin; // minimum dimension of indexed vectors in this cluster
    int indexedVectorDimMax; // maximum dimension of indexed vectors in this cluster

    DenseVectorFieldStats(String name) {
        super(name);
        indexedVectorCount = 0;
        indexedVectorDimMin = UNSET;
        indexedVectorDimMax = UNSET;
        vectorIndexTypeCount = new HashMap<>();
        vectorSimilarityTypeCount = new HashMap<>();
        vectorElementTypeCount = new HashMap<>();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert false : "writeTo should not be called on DenseVectorFieldStats";
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("indexed_vector_count", indexedVectorCount);
        builder.field("indexed_vector_dim_min", indexedVectorDimMin);
        builder.field("indexed_vector_dim_max", indexedVectorDimMax);
        if (vectorIndexTypeCount.isEmpty() == false) {
            builder.startObject("vector_index_type_count");
            builder.mapContents(vectorIndexTypeCount);
            builder.endObject();
        }
        if (vectorSimilarityTypeCount.isEmpty() == false) {
            builder.startObject("vector_similarity_type_count");
            builder.mapContents(vectorSimilarityTypeCount);
            builder.endObject();
        }
        if (vectorElementTypeCount.isEmpty() == false) {
            builder.startObject("vector_element_type_count");
            builder.mapContents(vectorElementTypeCount);
            builder.endObject();
        }
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
            && indexedVectorDimMax == that.indexedVectorDimMax
            && Objects.equals(vectorIndexTypeCount, that.vectorIndexTypeCount)
            && Objects.equals(vectorSimilarityTypeCount, that.vectorSimilarityTypeCount)
            && Objects.equals(vectorElementTypeCount, that.vectorElementTypeCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            indexedVectorCount,
            indexedVectorDimMin,
            indexedVectorDimMax,
            vectorIndexTypeCount,
            vectorSimilarityTypeCount,
            vectorElementTypeCount
        );
    }

    @Override
    public String toString() {
        return "DenseVectorFieldStats{"
            + "vectorIndexTypeCount="
            + vectorIndexTypeCount
            + ", vectorSimilarityTypeCount="
            + vectorSimilarityTypeCount
            + ", vectorElementTypeCount="
            + vectorElementTypeCount
            + ", indexedVectorCount="
            + indexedVectorCount
            + ", indexedVectorDimMin="
            + indexedVectorDimMin
            + ", indexedVectorDimMax="
            + indexedVectorDimMax
            + ", scriptCount="
            + scriptCount
            + ", scriptLangs="
            + scriptLangs
            + ", fieldScriptStats="
            + fieldScriptStats
            + ", name='"
            + name
            + '\''
            + ", count="
            + count
            + ", indexCount="
            + indexCount
            + '}';
    }
}
