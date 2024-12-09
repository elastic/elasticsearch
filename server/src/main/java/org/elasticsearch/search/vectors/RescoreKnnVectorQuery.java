/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.elasticsearch.index.mapper.vectors.VectorSimilarityFloatValueSource;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Wraps an internal query to rescore the results using a similarity function over the original, non-quantized vectors of a vector field
 */
public class RescoreKnnVectorQuery extends Query implements QueryProfilerProvider {
    private final String fieldName;
    private final float[] floatTarget;
    private final VectorSimilarityFunction vectorSimilarityFunction;
    private final Query innerQuery;

    private QueryProfilerProvider vectorProfiling;

    public RescoreKnnVectorQuery(
        String fieldName,
        float[] floatTarget,
        VectorSimilarityFunction vectorSimilarityFunction,
        Query innerQuery
    ) {
        this.fieldName = fieldName;
        this.floatTarget = floatTarget;
        this.vectorSimilarityFunction = vectorSimilarityFunction;
        this.innerQuery = innerQuery;
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        DoubleValuesSource valueSource = new VectorSimilarityFloatValueSource(fieldName, floatTarget, vectorSimilarityFunction);
        // Vector similarity VectorSimilarityFloatValueSource keep track of the compared vectors - we need that in case we don't need
        // to calculate top k and return directly the query to understand how many comparisons were done
        vectorProfiling = (QueryProfilerProvider) valueSource;
        FunctionScoreQuery functionScoreQuery = new FunctionScoreQuery(innerQuery, valueSource);
        return searcher.rewrite(functionScoreQuery);
    }

    public Query innerQuery() {
        return innerQuery;
    }

    @Override
    public void profile(QueryProfiler queryProfiler) {
        if (innerQuery instanceof QueryProfilerProvider queryProfilerProvider) {
            queryProfilerProvider.profile(queryProfiler);
        }

        if (vectorProfiling == null) {
            throw new IllegalStateException("Query should have been rewritten");
        }
        vectorProfiling.profile(queryProfiler);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        innerQuery.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RescoreKnnVectorQuery that = (RescoreKnnVectorQuery) o;
        return Objects.equals(fieldName, that.fieldName)
            && Objects.deepEquals(floatTarget, that.floatTarget)
            && vectorSimilarityFunction == that.vectorSimilarityFunction
            && Objects.equals(innerQuery, that.innerQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, Arrays.hashCode(floatTarget), vectorSimilarityFunction, innerQuery);
    }

    @Override
    public String toString(String field) {
        return "KnnRescoreVectorQuery{"
            + "fieldName='"
            + fieldName
            + '\''
            + ", floatTarget="
            + floatTarget[0]
            + "..."
            + ", vectorSimilarityFunction="
            + vectorSimilarityFunction
            + ", vectorQuery="
            + innerQuery
            + '}';
    }
}
