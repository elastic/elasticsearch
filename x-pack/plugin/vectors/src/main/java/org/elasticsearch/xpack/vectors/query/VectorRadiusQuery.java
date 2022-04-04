/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.DocIdSetBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Uses {@link KnnVectorsReader#search} to find the nearest neighbors within a given search radius.
 */
public class VectorRadiusQuery extends Query {
    private final String field;
    private final float[] origin;
    private final float radius;
    private final int numCands;

    /**
     * Find the documents with the nearest vectors to the origin vector within a search radius.
     * The radius is expressed in terms of the vector similarity (see {@link VectorSimilarityFunction}).
     *
     * @param field a field that has been indexed as a {@link KnnVectorField}.
     * @param origin the origin vector for the search
     * @param radius the search radius that the candidate vectors must fall in to be returned
     * @param numCands the number of nearest neighbor candidates to consider per segment
     */
    public VectorRadiusQuery(String field, float[] origin, float radius, int numCands) {
        this.field = field;
        this.origin = origin;
        this.radius = radius;
        this.numCands = numCands;
    }

    @Override
    public String toString(String field) {
        return getClass().getSimpleName() + ":" + this.field + "[" + origin[0] + ",...][" + radius + "][" + numCands + "]";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VectorRadiusQuery that = (VectorRadiusQuery) o;
        return Float.compare(that.radius, radius) == 0
            && numCands == that.numCands
            && Objects.equals(field, that.field)
            && Arrays.equals(origin, that.origin);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(field, radius, numCands);
        result = 31 * result + Arrays.hashCode(origin);
        return result;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        return new ConstantScoreWeight(this, boost) {

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                Bits acceptDocs = context.reader().getLiveDocs();
                TopDocs topDocs = context.reader().searchNearestVectors(field, origin, numCands, acceptDocs, Integer.MAX_VALUE);
                if (topDocs == null) {
                    return null;
                }

                // Only keep the vector candidates that satisfy the search radius
                FieldInfo fi = context.reader().getFieldInfos().fieldInfo(field);
                assert fi != null && fi.hasVectorValues();

                DocIdSetBuilder builder = new DocIdSetBuilder(context.reader().maxDoc());
                DocIdSetBuilder.BulkAdder adder = builder.grow(topDocs.scoreDocs.length);

                VectorSimilarityFunction similarityFunction = fi.getVectorSimilarityFunction();
                float minScore = similarityFunction.convertToScore(radius);
                for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                    if (scoreDoc.score >= minScore) {
                        adder.add(scoreDoc.doc);
                    }
                }

                DocIdSetIterator iterator = builder.build().iterator();
                return new ConstantScoreScorer(this, boost, scoreMode, iterator);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }
        };
    }
}
