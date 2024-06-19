/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Exact knn query. Will iterate and score all documents that have the provided knn field in the index.
 */
public abstract class ExactKnnQuery extends Query {

    protected final String field;

    public ExactKnnQuery(String field) {
        this.field = field;
    }

    @Override
    public void visit(QueryVisitor queryVisitor) {
        queryVisitor.visitLeaf(this);
    }

    abstract static class ExactKnnWeight extends Weight {
        private final String field;

        protected ExactKnnWeight(ExactKnnQuery query) {
            super(query);
            this.field = query.field;
        }

        abstract VectorScorer vectorScorer(LeafReaderContext leafReaderContext) throws IOException;

        @Override
        public Explanation explain(LeafReaderContext leafReaderContext, int i) throws IOException {
            VectorScorer vectorScorer = vectorScorer(leafReaderContext);
            if (vectorScorer == null) {
                return Explanation.noMatch("No vector values found for field: " + field);
            }
            DocIdSetIterator iterator = vectorScorer.iterator();
            iterator.advance(i);
            if (iterator.docID() == i) {
                return Explanation.match(vectorScorer.score(), "ExactKnnQuery");
            }
            return Explanation.noMatch("Document not found in vector values for field: " + field);
        }

        @Override
        public Scorer scorer(LeafReaderContext leafReaderContext) throws IOException {
            VectorScorer vectorScorer = vectorScorer(leafReaderContext);
            if (vectorScorer == null) {
                return null;
            }
            return new ExactKnnScorer(this, vectorScorer);
        }

        @Override
        public boolean isCacheable(LeafReaderContext leafReaderContext) {
            return true;
        }
    }

    public static class Floats extends ExactKnnQuery {

        private final float[] query;

        public Floats(float[] query, String field) {
            super(field);
            this.query = query;
        }

        public float[] getQuery() {
            return query;
        }

        @Override
        public String toString(String field) {
            return "ExactKnnQuery.Floats";
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new ExactKnnWeight(Floats.this) {
                @Override
                VectorScorer vectorScorer(LeafReaderContext leafReaderContext) throws IOException {
                    FloatVectorValues vectorValues = leafReaderContext.reader().getFloatVectorValues(field);
                    if (vectorValues == null) {
                        return null;
                    }
                    return vectorValues.scorer(query);
                }
            };
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Floats floats = (Floats) o;
            return Objects.equals(field, floats.field) && Objects.deepEquals(query, floats.query);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, Arrays.hashCode(query));
        }
    }

    public static class Bytes extends ExactKnnQuery {

        private final byte[] query;

        public Bytes(byte[] query, String field) {
            super(field);
            this.query = query;
        }

        @Override
        public String toString(String field) {
            return "ExactKnnQuery.Floats";
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            return new ExactKnnWeight(Bytes.this) {
                @Override
                VectorScorer vectorScorer(LeafReaderContext leafReaderContext) throws IOException {
                    ByteVectorValues vectorValues = leafReaderContext.reader().getByteVectorValues(field);
                    if (vectorValues == null) {
                        return null;
                    }
                    return vectorValues.scorer(query);
                }
            };
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Bytes bytes = (Bytes) o;
            return Objects.equals(field, bytes.field) && Objects.deepEquals(query, bytes.query);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field, Arrays.hashCode(query));
        }
    }

    static class ExactKnnScorer extends Scorer {

        private final VectorScorer vectorScorer;
        private final DocIdSetIterator iterator;

        ExactKnnScorer(Weight weight, VectorScorer vectorScorer) {
            super(weight);
            this.vectorScorer = vectorScorer;
            this.iterator = vectorScorer.iterator();
        }

        @Override
        public DocIdSetIterator iterator() {
            return vectorScorer.iterator();
        }

        @Override
        public float getMaxScore(int i) throws IOException {
            return Float.POSITIVE_INFINITY;
        }

        @Override
        public float score() throws IOException {
            assert iterator.docID() != -1;
            return vectorScorer.score();
        }

        @Override
        public int docID() {
            return iterator.docID();
        }
    }
}
