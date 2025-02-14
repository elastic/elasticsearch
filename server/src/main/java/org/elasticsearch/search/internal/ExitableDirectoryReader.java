/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.search.suggest.document.CompletionTerms;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;

import java.io.IOException;
import java.util.Objects;

/**
 * Wraps an {@link IndexReader} with a {@link QueryCancellation}
 * which checks for cancelled or timed-out query.
 * Note: this class was adapted from Lucene's ExitableDirectoryReader, but instead of using a query timeout for cancellation,
 *       a {@link QueryCancellation} object is used. The main behavior of the classes is mostly unchanged.
 */
class ExitableDirectoryReader extends FilterDirectoryReader {

    /**
     * Used to check if query cancellation is actually enabled
     * and if so use it to check if the query is cancelled or timed-out.
     */
    interface QueryCancellation {

        /**
         * Used to prevent unnecessary checks for cancellation
         * @return true if query cancellation is enabled
         */
        boolean isEnabled();

        /**
         * Call to check if the query is cancelled or timed-out.
         * If so a {@link RuntimeException} is thrown
         */
        void checkCancelled();
    }

    ExitableDirectoryReader(DirectoryReader in, QueryCancellation queryCancellation) throws IOException {
        super(in, new SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader reader) {
                return new ExitableLeafReader(reader, queryCancellation);
            }
        });
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) {
        throw new UnsupportedOperationException("doWrapDirectoryReader() should never be invoked");
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return in.getReaderCacheHelper();
    }

    /**
     * Wraps a {@link FilterLeafReader} with a {@link QueryCancellation}.
     */
    static class ExitableLeafReader extends SequentialStoredFieldsLeafReader {

        private final QueryCancellation queryCancellation;

        private ExitableLeafReader(LeafReader leafReader, QueryCancellation queryCancellation) {
            super(leafReader);
            this.queryCancellation = queryCancellation;
        }

        @Override
        public PointValues getPointValues(String field) throws IOException {
            final PointValues pointValues = in.getPointValues(field);
            if (pointValues == null) {
                return null;
            }
            return queryCancellation.isEnabled() ? new ExitablePointValues(pointValues, queryCancellation) : pointValues;
        }

        @Override
        public Terms terms(String field) throws IOException {
            Terms terms = in.terms(field);
            if (terms == null) {
                return null;
            }
            // If we have a suggest CompletionQuery then the CompletionWeight#bulkScorer() will check that
            // the terms are instanceof CompletionTerms (not generic FilterTerms) and will throw an exception
            // if that's not the case.
            return (queryCancellation.isEnabled() && terms instanceof CompletionTerms == false)
                ? new ExitableTerms(terms, queryCancellation)
                : terms;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }

        @Override
        protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
            return reader;
        }

        @Override
        public ByteVectorValues getByteVectorValues(String field) throws IOException {
            ByteVectorValues vectorValues = in.getByteVectorValues(field);
            if (vectorValues == null) {
                return null;
            }
            return queryCancellation.isEnabled() ? new ExitableByteVectorValues(queryCancellation, vectorValues) : vectorValues;
        }

        @Override
        public void searchNearestVectors(String field, byte[] target, KnnCollector collector, Bits acceptDocs) throws IOException {
            if (queryCancellation.isEnabled() == false) {
                in.searchNearestVectors(field, target, collector, acceptDocs);
                return;
            }
            in.searchNearestVectors(field, target, collector, new TimeOutCheckingBits(acceptDocs));
        }

        @Override
        public FloatVectorValues getFloatVectorValues(String field) throws IOException {
            FloatVectorValues vectorValues = in.getFloatVectorValues(field);
            if (vectorValues == null) {
                return null;
            }
            return queryCancellation.isEnabled() ? new ExitableFloatVectorValues(vectorValues, queryCancellation) : vectorValues;
        }

        @Override
        public void searchNearestVectors(String field, float[] target, KnnCollector collector, Bits acceptDocs) throws IOException {
            if (queryCancellation.isEnabled() == false) {
                in.searchNearestVectors(field, target, collector, acceptDocs);
                return;
            }
            in.searchNearestVectors(field, target, collector, new TimeOutCheckingBits(acceptDocs));
        }

        private class TimeOutCheckingBits implements Bits {
            private static final int MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK = 10;
            private final Bits updatedAcceptDocs;
            private int calls;

            TimeOutCheckingBits(Bits acceptDocs) {
                // when acceptDocs is null due to no doc deleted, we will instantiate a new one that would
                // match all docs to allow timeout checking.
                this.updatedAcceptDocs = acceptDocs == null ? new Bits.MatchAllBits(maxDoc()) : acceptDocs;
            }

            @Override
            public boolean get(int index) {
                if (calls++ % MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK == 0) {
                    queryCancellation.checkCancelled();
                }
                return updatedAcceptDocs.get(index);
            }

            @Override
            public int length() {
                return updatedAcceptDocs.length();
            }
        }
    }

    /**
     * Wrapper class for {@link FilterLeafReader.FilterTerms} that check for query cancellation or timeout.
     */
    static class ExitableTerms extends FilterLeafReader.FilterTerms {

        private final QueryCancellation queryCancellation;

        ExitableTerms(Terms terms, QueryCancellation queryCancellation) {
            super(terms);
            this.queryCancellation = queryCancellation;
        }

        @Override
        public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
            return new ExitableTermsEnum(in.intersect(compiled, startTerm), queryCancellation);
        }

        @Override
        public TermsEnum iterator() throws IOException {
            return new ExitableTermsEnum(in.iterator(), queryCancellation);
        }

        @Override
        public BytesRef getMin() throws IOException {
            return in.getMin();
        }

        @Override
        public BytesRef getMax() throws IOException {
            return in.getMax();
        }
    }

    /**
     * Wrapper class for {@link FilterLeafReader.FilterTermsEnum} that is used by {@link ExitableTerms} for
     * implementing an exitable enumeration of terms.
     */
    private static class ExitableTermsEnum extends FilterLeafReader.FilterTermsEnum {

        private static final int MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK = (1 << 4) - 1; // 15

        private int calls;
        private final QueryCancellation queryCancellation;

        private ExitableTermsEnum(TermsEnum termsEnum, QueryCancellation queryCancellation) {
            super(termsEnum);
            this.queryCancellation = queryCancellation;
            this.queryCancellation.checkCancelled();
        }

        private void checkAndThrowWithSampling() {
            if ((calls++ & MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK) == 0) {
                queryCancellation.checkCancelled();
            }
        }

        @Override
        public BytesRef next() throws IOException {
            checkAndThrowWithSampling();
            return in.next();
        }
    }

    /**
     * Wrapper class for {@link PointValues} that checks for query cancellation or timeout.
     */
    static class ExitablePointValues extends PointValues {

        private final PointValues in;
        private final QueryCancellation queryCancellation;

        private ExitablePointValues(PointValues in, QueryCancellation queryCancellation) {
            this.in = in;
            this.queryCancellation = queryCancellation;
            this.queryCancellation.checkCancelled();
        }

        @Override
        public PointTree getPointTree() throws IOException {
            queryCancellation.checkCancelled();
            return new ExitablePointTree(in, in.getPointTree(), queryCancellation);
        }

        @Override
        public byte[] getMinPackedValue() throws IOException {
            queryCancellation.checkCancelled();
            return in.getMinPackedValue();
        }

        @Override
        public byte[] getMaxPackedValue() throws IOException {
            queryCancellation.checkCancelled();
            return in.getMaxPackedValue();
        }

        @Override
        public int getNumDimensions() throws IOException {
            queryCancellation.checkCancelled();
            return in.getNumDimensions();
        }

        @Override
        public int getNumIndexDimensions() throws IOException {
            queryCancellation.checkCancelled();
            return in.getNumIndexDimensions();
        }

        @Override
        public int getBytesPerDimension() throws IOException {
            queryCancellation.checkCancelled();
            return in.getBytesPerDimension();
        }

        @Override
        public long size() {
            queryCancellation.checkCancelled();
            return in.size();
        }

        @Override
        public int getDocCount() {
            queryCancellation.checkCancelled();
            return in.getDocCount();
        }
    }

    private static class ExitablePointTree implements PointValues.PointTree {

        private final PointValues pointValues;
        private final PointValues.PointTree in;
        private final ExitableIntersectVisitor exitableIntersectVisitor;
        private final QueryCancellation queryCancellation;
        private int calls;

        private ExitablePointTree(PointValues pointValues, PointValues.PointTree in, QueryCancellation queryCancellation) {
            this.pointValues = pointValues;
            this.in = in;
            this.queryCancellation = queryCancellation;
            this.exitableIntersectVisitor = new ExitableIntersectVisitor(queryCancellation);
        }

        /**
         * Throws {@link org.apache.lucene.index.ExitableDirectoryReader.ExitingReaderException}
         * if {@link QueryTimeout#shouldExit()} returns true, or
         * if {@link Thread#interrupted()} returns true.
         */
        private void checkAndThrowWithSampling() {
            if ((calls++ & ExitableIntersectVisitor.MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK) == 0) {
                queryCancellation.checkCancelled();
            }
        }

        @Override
        public PointValues.PointTree clone() {
            queryCancellation.checkCancelled();
            return new ExitablePointTree(pointValues, in.clone(), queryCancellation);
        }

        @Override
        public boolean moveToChild() throws IOException {
            checkAndThrowWithSampling();
            return in.moveToChild();
        }

        @Override
        public boolean moveToSibling() throws IOException {
            checkAndThrowWithSampling();
            return in.moveToSibling();
        }

        @Override
        public boolean moveToParent() throws IOException {
            checkAndThrowWithSampling();
            return in.moveToParent();
        }

        @Override
        public byte[] getMinPackedValue() {
            checkAndThrowWithSampling();
            return in.getMinPackedValue();
        }

        @Override
        public byte[] getMaxPackedValue() {
            checkAndThrowWithSampling();
            return in.getMaxPackedValue();
        }

        @Override
        public long size() {
            queryCancellation.checkCancelled();
            return in.size();
        }

        @Override
        public void visitDocIDs(PointValues.IntersectVisitor visitor) throws IOException {
            queryCancellation.checkCancelled();
            in.visitDocIDs(visitor);
        }

        @Override
        public void visitDocValues(PointValues.IntersectVisitor visitor) throws IOException {
            queryCancellation.checkCancelled();
            exitableIntersectVisitor.setIntersectVisitor(visitor);
            in.visitDocValues(exitableIntersectVisitor);
        }
    }

    private static class ExitableIntersectVisitor implements PointValues.IntersectVisitor {

        static final int MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK = (1 << 13) - 1; // 8191

        private final QueryCancellation queryCancellation;
        private PointValues.IntersectVisitor in;
        private int calls;

        private ExitableIntersectVisitor(QueryCancellation queryCancellation) {
            this.queryCancellation = queryCancellation;
        }

        private void setIntersectVisitor(PointValues.IntersectVisitor in) {
            this.in = in;
        }

        private void checkAndThrowWithSampling() {
            if ((calls++ & MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK) == 0) {
                queryCancellation.checkCancelled();
            }
        }

        @Override
        public void visit(int docID) throws IOException {
            checkAndThrowWithSampling();
            in.visit(docID);
        }

        @Override
        public void visit(DocIdSetIterator iterator) throws IOException {
            checkAndThrowWithSampling();
            in.visit(iterator);
        }

        @Override
        public void visit(int docID, byte[] packedValue) throws IOException {
            checkAndThrowWithSampling();
            in.visit(docID, packedValue);
        }

        @Override
        public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
            checkAndThrowWithSampling();
            in.visit(iterator, packedValue);
        }

        @Override
        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            queryCancellation.checkCancelled();
            return in.compare(minPackedValue, maxPackedValue);
        }

        @Override
        public void grow(int count) {
            queryCancellation.checkCancelled();
            in.grow(count);
        }
    }

    private static class ExitableByteVectorValues extends ByteVectorValues {
        private final QueryCancellation queryCancellation;
        private final ByteVectorValues in;

        private ExitableByteVectorValues(QueryCancellation queryCancellation, ByteVectorValues in) {
            this.queryCancellation = queryCancellation;
            this.in = in;
        }

        @Override
        public int dimension() {
            return in.dimension();
        }

        @Override
        public int size() {
            return in.size();
        }

        @Override
        public byte[] vectorValue(int ord) throws IOException {
            return in.vectorValue(ord);
        }

        @Override
        public int ordToDoc(int ord) {
            return in.ordToDoc(ord);
        }

        @Override
        public VectorScorer scorer(byte[] bytes) throws IOException {
            VectorScorer scorer = in.scorer(bytes);
            if (scorer == null) {
                return null;
            }
            return new VectorScorer() {
                private final DocIdSetIterator iterator = new ExitableDocSetIterator(scorer.iterator(), queryCancellation);

                @Override
                public float score() throws IOException {
                    return scorer.score();
                }

                @Override
                public DocIdSetIterator iterator() {
                    return iterator;
                }
            };
        }

        @Override
        public DocIndexIterator iterator() {
            return createExitableIterator(in.iterator(), queryCancellation);
        }

        @Override
        public ByteVectorValues copy() throws IOException {
            return in.copy();
        }
    }

    private static class ExitableFloatVectorValues extends FilterFloatVectorValues {
        private final QueryCancellation queryCancellation;

        ExitableFloatVectorValues(FloatVectorValues vectorValues, QueryCancellation queryCancellation) {
            super(vectorValues);
            this.queryCancellation = queryCancellation;
            this.queryCancellation.checkCancelled();
        }

        @Override
        public float[] vectorValue(int ord) throws IOException {
            return in.vectorValue(ord);
        }

        @Override
        public int ordToDoc(int ord) {
            return in.ordToDoc(ord);
        }

        @Override
        public VectorScorer scorer(float[] target) throws IOException {
            VectorScorer scorer = in.scorer(target);
            if (scorer == null) {
                return null;
            }
            return new VectorScorer() {
                private final DocIdSetIterator iterator = new ExitableDocSetIterator(scorer.iterator(), queryCancellation);

                @Override
                public float score() throws IOException {
                    return scorer.score();
                }

                @Override
                public DocIdSetIterator iterator() {
                    return iterator;
                }
            };
        }

        @Override
        public DocIndexIterator iterator() {
            return createExitableIterator(in.iterator(), queryCancellation);
        }

        @Override
        public FloatVectorValues copy() throws IOException {
            return in.copy();
        }
    }

    private static KnnVectorValues.DocIndexIterator createExitableIterator(
        KnnVectorValues.DocIndexIterator delegate,
        QueryCancellation queryCancellation
    ) {
        return new KnnVectorValues.DocIndexIterator() {
            private int calls;

            @Override
            public int index() {
                return delegate.index();
            }

            @Override
            public int docID() {
                return delegate.docID();
            }

            @Override
            public long cost() {
                return delegate.cost();
            }

            @Override
            public int nextDoc() throws IOException {
                int nextDoc = delegate.nextDoc();
                checkAndThrowWithSampling();
                return nextDoc;
            }

            @Override
            public int advance(int target) throws IOException {
                final int advance = delegate.advance(target);
                checkAndThrowWithSampling();
                return advance;
            }

            private void checkAndThrowWithSampling() {
                if ((calls++ & ExitableIntersectVisitor.MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK) == 0) {
                    queryCancellation.checkCancelled();
                }
            }
        };
    }

    private static class ExitableDocSetIterator extends DocIdSetIterator {
        private int calls;
        private final DocIdSetIterator in;
        private final QueryCancellation queryCancellation;

        private ExitableDocSetIterator(DocIdSetIterator in, QueryCancellation queryCancellation) {
            this.in = in;
            this.queryCancellation = queryCancellation;
        }

        @Override
        public int docID() {
            return in.docID();
        }

        @Override
        public int advance(int target) throws IOException {
            final int advance = in.advance(target);
            checkAndThrowWithSampling();
            return advance;
        }

        @Override
        public int nextDoc() throws IOException {
            final int nextDoc = in.nextDoc();
            checkAndThrowWithSampling();
            return nextDoc;
        }

        @Override
        public long cost() {
            return in.cost();
        }

        private void checkAndThrowWithSampling() {
            if ((calls++ & ExitableIntersectVisitor.MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK) == 0) {
                this.queryCancellation.checkCancelled();
            }
        }
    }

    /** Delegates all methods to a wrapped {@link FloatVectorValues}. */
    private abstract static class FilterFloatVectorValues extends FloatVectorValues {

        /** Wrapped values */
        protected final FloatVectorValues in;

        /** Sole constructor */
        protected FilterFloatVectorValues(FloatVectorValues in) {
            Objects.requireNonNull(in);
            this.in = in;
        }

        @Override
        public DocIndexIterator iterator() {
            return in.iterator();
        }

        @Override
        public float[] vectorValue(int ord) throws IOException {
            return in.vectorValue(ord);
        }

        @Override
        public FloatVectorValues copy() throws IOException {
            return in.copy();
        }

        @Override
        public int dimension() {
            return in.dimension();
        }

        @Override
        public int size() {
            return in.size();
        }

    }
}
