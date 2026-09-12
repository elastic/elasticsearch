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
 * Holds enhanced stats about a rank vectors mapped field.
 * <p>
 * {@code rank_vectors} fields are always doc-values only and brute-force scored, so unlike
 * {@link DenseVectorFieldStats} there is no index type or similarity to report on. What is worth tracking is the
 * element type mix (which determines the on-disk encoding) and the range of dimensions in use.
 */
public final class RankVectorsFieldStats extends FieldStats {
    static final int UNSET = -1;
    Map<String, Integer> vectorElementTypeCount; // count of mappings by element type
    int vectorDimMin; // minimum dimension of the vectors in this cluster
    int vectorDimMax; // maximum dimension of the vectors in this cluster

    RankVectorsFieldStats(String name) {
        super(name);
        vectorDimMin = UNSET;
        vectorDimMax = UNSET;
        vectorElementTypeCount = new HashMap<>();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert false : "writeTo should not be called on RankVectorsFieldStats";
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("vector_dim_min", vectorDimMin);
        builder.field("vector_dim_max", vectorDimMax);
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
        RankVectorsFieldStats that = (RankVectorsFieldStats) o;
        return vectorDimMin == that.vectorDimMin
            && vectorDimMax == that.vectorDimMax
            && Objects.equals(vectorElementTypeCount, that.vectorElementTypeCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), vectorDimMin, vectorDimMax, vectorElementTypeCount);
    }

    @Override
    public String toString() {
        return "RankVectorsFieldStats{"
            + "vectorElementTypeCount="
            + vectorElementTypeCount
            + ", vectorDimMin="
            + vectorDimMin
            + ", vectorDimMax="
            + vectorDimMax
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
