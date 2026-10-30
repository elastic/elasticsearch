/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.chunks;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.DenseVectorFieldType;
import org.elasticsearch.search.vectors.DenseVectorQuery;
import org.elasticsearch.search.vectors.IVFKnnFloatVectorQuery;
import org.elasticsearch.search.vectors.RescoreKnnVectorQuery;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.search.vectors.VectorSimilarityQuery;

import java.util.List;

/**
 * Collects exact knn queries for the knn queries on {@code fieldType}, applying the similarity threshold of the enclosing
 * {@link VectorSimilarityQuery} if any.
 */
final class DenseVectorQueryExtractor extends QueryVisitor {
    private final DenseVectorFieldType fieldType;
    private final List<Query> queries;
    private final Float similarity;

    DenseVectorQueryExtractor(DenseVectorFieldType fieldType, List<Query> queries, Float similarity) {
        this.fieldType = fieldType;
        this.queries = queries;
        this.similarity = similarity;
    }

    @Override
    public boolean acceptField(String field) {
        return fieldType.name().equals(field);
    }

    @Override
    public void consumeTerms(Query query, Term... terms) {
        super.consumeTerms(query, terms);
    }

    @Override
    public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
        if (parent instanceof VectorSimilarityQuery similarityQuery) {
            return new DenseVectorQueryExtractor(fieldType, queries, similarityQuery.getSimilarity());
        }
        return this;
    }

    private void visitLeaf(Query query, Float similarity) {
        if (query instanceof KnnFloatVectorQuery knnQuery) {
            queries.add(fieldType.createIndexedExactKnnQuery(VectorData.fromFloats(knnQuery.getTargetCopy()), similarity, null));
        } else if (query instanceof KnnByteVectorQuery knnQuery) {
            queries.add(fieldType.createIndexedExactKnnQuery(VectorData.fromBytes(knnQuery.getTargetCopy()), similarity, null));
        } else if (query instanceof MatchAllDocsQuery) {
            queries.add(Queries.ALL_DOCS_INSTANCE);
        } else if (query instanceof DenseVectorQuery.Floats floatsQuery) {
            queries.add(fieldType.createIndexedExactKnnQuery(VectorData.fromFloats(floatsQuery.getQuery()), similarity, null));
        } else if (query instanceof IVFKnnFloatVectorQuery ivfQuery) {
            queries.add(fieldType.createIndexedExactKnnQuery(VectorData.fromFloats(ivfQuery.getQuery()), similarity, null));
        } else if (query instanceof RescoreKnnVectorQuery rescoreQuery) {
            visitLeaf(rescoreQuery.innerQuery(), similarity);
        }
    }

    @Override
    public void visitLeaf(Query query) {
        visitLeaf(query, similarity);
    }
}
