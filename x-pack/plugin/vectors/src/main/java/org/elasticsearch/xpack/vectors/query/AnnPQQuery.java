/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.vectors.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.vectors.codec.VectorDocValues;
import org.elasticsearch.xpack.vectors.mapper.DenseVectorFieldMapper.DenseVectorFieldType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;


public class AnnPQQuery extends Query {
    final DenseVectorFieldType fieldType;
    final float[] queryVector;
    final int pqCount;
    final int pdims;
    final float[] qSquareMagnitudes;

    public AnnPQQuery(DenseVectorFieldType fieldType, float[] queryVector) {
        this.fieldType = fieldType;
        this.queryVector = queryVector;
        this.pqCount = fieldType.pqCount();
        this.pdims = fieldType.dims()/pqCount; // number of dimensions in each product centroid

        // calculate query square magnitudes broken by quantizers
        this.qSquareMagnitudes = new float[pqCount];
        for (int pq = 0; pq < pqCount; pq++) {
            for (int dim = pq * pdims; dim < (pq * pdims + pdims); dim++) {
                qSquareMagnitudes[pq] += queryVector[dim] * queryVector[dim];
            }
        }
    }

    //TODO: handle deleted docs, intersect live docs with vectordocValues
    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new Weight(this) {
            @Override
            public void extractTerms(Set<Term> terms) {}

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                final VectorDocValues values = (VectorDocValues) context.reader().getBinaryDocValues(fieldType.name());
                return new ANNPQScorer(this, values);
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                return null;
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }
        };
    }

    private class ANNPQScorer extends Scorer {
        private final VectorDocValues values;
        private final float[][] cqDistances; // squared distances between queryVector and all product centroids

        private ANNPQScorer(Weight w, VectorDocValues values) throws IOException {
            super(w);
            this.values = values;

            // precalculate squared distances between queryVector and all product centroids
            // we use euclidean distance for this
            // ((c1-q1)^2 + (c2-q2)^2 ...) can be converted to (||c||^2 + ||q||^2 - 2*c*q)
            // where ||c||^2 -- squared magnitude of product centroid
            // where ||q||^2 -- squared magnitude of query vector
            float[][][] pCentroids = values.pcentroids();
            float[][] pcSquaredMagnitudes = values.pCentroidsSquaredMagnitudes();
            int pcentroidsCount = values.pcentroidsCount();

            this.cqDistances = new float[pqCount][pcentroidsCount];
            for (int pq = 0; pq < pqCount; pq++) {
                for (int c = 0; c < pcentroidsCount; c++) {
                    float dotProduct = 0;
                    for (int dim = 0; dim < pdims; dim++) {
                        dotProduct += queryVector[pq * pdims + dim] * pCentroids[pq][c][dim];
                    }
                    cqDistances[pq][c] = pcSquaredMagnitudes[pq][c] + qSquareMagnitudes[pq] - 2 * dotProduct;
                }
            }
        }

        @Override
        public final int docID() {
            return values.docID();
        }

        @Override
        public float score() throws IOException {
            BytesRef valueBR = values.docCentroids();
            int offset = valueBR.offset;
            float score = 0;
            for (int i = 0; i < pqCount; i++) {
                int docCentroid = valueBR.bytes[offset++] & 0xFF; // centroids are stored as unsigned bytes
                score += cqDistances[i][docCentroid];
            }
            score = 1/score; // as we want to get closest vectors fist, score is inversely proportional to distance
            return score;
        }

        @Override
        public final DocIdSetIterator iterator() {
            return values;
        }

        @Override
        public float getMaxScore(int upTo) {
            return Float.MAX_VALUE;
        }
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AnnPQQuery other = (AnnPQQuery) obj;
        return Objects.equals(fieldType, other.fieldType)
            && Arrays.equals(queryVector, other.queryVector);
    }

    @Override
    public String toString(String field) {
        String s = "annpq  (field:" + fieldType.name() + ")";
        return s;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldType.hashCode(), Arrays.hashCode(queryVector));
    }
}
