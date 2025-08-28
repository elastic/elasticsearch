/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Enhanced FunctionScoreQuery that enables bulk vector processing for KNN rescoring.
 * When provided with a ScoreDoc array, performs bulk vector loading and similarity
 * computation instead of individual per-document processing.
 */
public class BulkVectorFunctionScoreQuery extends Query {

    private final Query subQuery;
    private final AccessibleVectorSimilarityFloatValueSource valueSource;
    private final ScoreDoc[] scoreDocs;

    public BulkVectorFunctionScoreQuery(Query subQuery, AccessibleVectorSimilarityFloatValueSource valueSource, ScoreDoc[] scoreDocs) {
        this.subQuery = subQuery;
        this.valueSource = valueSource;
        this.scoreDocs = scoreDocs;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        // TODO: take a closer look at ScoreMode
        Weight subQueryWeight = subQuery.createWeight(searcher, scoreMode, boost);
        return new BulkVectorFunctionScoreWeight(this, subQueryWeight, valueSource, scoreDocs);
    }

    @Override
    public Query rewrite(IndexSearcher searcher) throws IOException {
        Query rewrittenSubQuery = subQuery.rewrite(searcher);
        if (rewrittenSubQuery != subQuery) {
            return new BulkVectorFunctionScoreQuery(rewrittenSubQuery, valueSource, scoreDocs);
        }
        return this;
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("bulk_vector_function_score(");
        sb.append(subQuery.toString(field));
        sb.append(", vector_similarity=").append(valueSource.toString());
        if (scoreDocs != null) {
            sb.append(", bulk_docs=").append(scoreDocs.length);
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        BulkVectorFunctionScoreQuery that = (BulkVectorFunctionScoreQuery) obj;
        return Objects.equals(subQuery, that.subQuery)
            && Objects.equals(valueSource, that.valueSource)
            && Arrays.equals(scoreDocs, that.scoreDocs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subQuery, valueSource, scoreDocs);
    }

    @Override
    public void visit(org.apache.lucene.search.QueryVisitor visitor) {
        subQuery.visit(visitor.getSubVisitor(org.apache.lucene.search.BooleanClause.Occur.MUST, this));
    }
}
