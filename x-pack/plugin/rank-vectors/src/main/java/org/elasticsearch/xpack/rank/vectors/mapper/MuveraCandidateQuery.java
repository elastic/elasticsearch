/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.mapper;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.elasticsearch.search.internal.ContextIndexSearcher;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Acquires raw MUVERA candidates without exact rescoring. This query exists only for the
 * private evaluation action and is deliberately not exposed through public query XContent.
 */
public final class MuveraCandidateQuery extends Query {

    private static final Runnable NO_CANCELLATION = () -> {};

    private final String field;
    private final float[][] queryVectors;
    private final int numCandidates;

    public MuveraCandidateQuery(String field, float[][] queryVectors, int numCandidates) {
        this.field = Objects.requireNonNull(field);
        this.queryVectors = deepCopy(queryVectors);
        this.numCandidates = numCandidates;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        Runnable checkCancelled = searcher instanceof ContextIndexSearcher contextSearcher
            ? contextSearcher::checkCancelled
            : NO_CANCELLATION;
        return new KnnFloatVectorQuery(field, RankVectorsFieldMapper.encodeMuveraQuery(queryVectors, checkCancelled), numCandidates)
            .rewrite(searcher);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
    }

    @Override
    public String toString(String field) {
        return "MuveraCandidateQuery(field=" + this.field + ", numCandidates=" + numCandidates + ")";
    }

    @Override
    public boolean equals(Object object) {
        if (sameClassAs(object) == false) {
            return false;
        }
        MuveraCandidateQuery other = (MuveraCandidateQuery) object;
        return field.equals(other.field) && Arrays.deepEquals(queryVectors, other.queryVectors) && numCandidates == other.numCandidates;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, Arrays.deepHashCode(queryVectors), numCandidates);
    }

    private static float[][] deepCopy(float[][] vectors) {
        float[][] copy = new float[vectors.length][];
        for (int i = 0; i < vectors.length; i++) {
            copy[i] = vectors[i].clone();
        }
        return copy;
    }
}
