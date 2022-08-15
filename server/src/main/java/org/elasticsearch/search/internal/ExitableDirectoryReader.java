/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.suggest.document.CompletionTerms;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.elasticsearch.common.lucene.index.SequentialStoredFieldsLeafReader;

import java.io.IOException;

/**
 * Wraps an {@link IndexReader} with a {@link QueryCancellation}
 * which checks for cancelled or timed-out query.
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
    }

    /**
     * Wrapper class for {@link FilterLeafReader.FilterTerms} that check for query cancellation or timeout.
     */
    static class ExitableTerms extends FilterLeafReader.FilterTerms {

        private final QueryCancellation queryCancellation;

        private ExitableTerms(Terms terms, QueryCancellation queryCancellation) {
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
}
