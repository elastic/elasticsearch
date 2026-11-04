/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.mapper;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.vectors.IVFKnnFloatVectorQuery;
import org.elasticsearch.xpack.rank.vectors.fde.MuveraEncoder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DEFAULT_OVERSAMPLE;

/**
 * Acquires raw MUVERA candidates without exact rescoring. This query exists only for the
 * private evaluation action and is deliberately not exposed through public query XContent.
 */
public final class MuveraCandidateQuery extends Query {

    private static final Runnable NO_CANCELLATION = () -> {};

    private final String field;
    private final float[][] queryVectors;
    private final int k;
    private final int numCandidates;
    @Nullable
    private final Query filter;
    private final MuveraEncoder encoder;

    public MuveraCandidateQuery(
        String field,
        float[][] queryVectors,
        int k,
        int numCandidates,
        @Nullable Query filter,
        MuveraEncoder encoder
    ) {
        if (k < 1) {
            throw new IllegalArgumentException("[k] must be positive");
        }
        if (numCandidates < k) {
            throw new IllegalArgumentException("[num_candidates] must be greater than or equal to [k]");
        }
        this.field = Objects.requireNonNull(field);
        this.queryVectors = deepCopy(queryVectors);
        this.k = k;
        this.numCandidates = numCandidates;
        this.filter = filter;
        this.encoder = Objects.requireNonNull(encoder);
        this.encoder.validateQueryVectors(this.queryVectors);
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        Runnable checkCancelled = searcher instanceof ContextIndexSearcher contextSearcher
            ? contextSearcher::checkCancelled
            : NO_CANCELLATION;
        return approximateQuery(field, encoder.encodeQuery(queryVectors, checkCancelled), k, numCandidates, filter).rewrite(searcher);
    }

    static Query approximateQuery(String field, float[] queryFde, int k, int numCandidates, @Nullable Query filter) {
        return IVFKnnFloatVectorQuery.fromMappingConfig(
            field,
            queryFde,
            k,
            numCandidates,
            filter,
            0f,
            false,
            false,
            1,
            DEFAULT_OVERSAMPLE,
            null
        );
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public String toString(String field) {
        return "MuveraCandidateQuery(field=" + this.field + ", k=" + k + ", numCandidates=" + numCandidates + ")";
    }

    @Override
    public boolean equals(Object object) {
        if (sameClassAs(object) == false) {
            return false;
        }
        MuveraCandidateQuery other = (MuveraCandidateQuery) object;
        return field.equals(other.field)
            && Arrays.deepEquals(queryVectors, other.queryVectors)
            && k == other.k
            && numCandidates == other.numCandidates
            && Objects.equals(filter, other.filter)
            && encoder == other.encoder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, Arrays.deepHashCode(queryVectors), k, numCandidates, filter, encoder);
    }

    private static float[][] deepCopy(float[][] vectors) {
        float[][] copy = new float[vectors.length][];
        for (int i = 0; i < vectors.length; i++) {
            copy[i] = vectors[i].clone();
        }
        return copy;
    }
}
