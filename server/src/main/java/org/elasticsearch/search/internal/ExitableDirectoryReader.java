/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.suggest.document.CompletionTerms;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;

import java.io.IOException;

/**
 * Wraps an {@link IndexReader} with a {@link QueryTimeout}
 * which checks for cancelled or timed-out query.
 */
class ExitableDirectoryReader extends FilterDirectoryReader {

    ExitableDirectoryReader(DirectoryReader in, QueryTimeout queryTimeout) throws IOException {
        super(in, new SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader reader) {
                return new ExitableLeafReader(reader, queryTimeout);
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
     * Wraps a {@link FilterLeafReader} with a {@link QueryTimeout}.
     */
    static class ExitableLeafReader extends FilterLeafReader {

        private final QueryTimeout queryTimeout;

        private ExitableLeafReader(LeafReader leafReader, QueryTimeout queryTimeout) {
            super(leafReader);
            this.queryTimeout = queryTimeout;
        }

        @Override
        public PointValues getPointValues(String field) throws IOException {
            final PointValues pointValues = in.getPointValues(field);
            if (pointValues == null) {
                return null;
            }
            return queryTimeout.isTimeoutEnabled() ? new ExitablePointValues(pointValues, queryTimeout) : pointValues;
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
            return (queryTimeout.isTimeoutEnabled() && terms instanceof CompletionTerms == false) ?
                    new ExitableTerms(terms, queryTimeout) : terms;
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    /**
     * Wrapper class for {@link FilterLeafReader.FilterTerms} that check for query cancellation or timeout.
     */
    static class ExitableTerms extends FilterLeafReader.FilterTerms {

        private final QueryTimeout queryTimeout;

        private ExitableTerms(Terms terms, QueryTimeout queryTimeout) {
            super(terms);
            this.queryTimeout = queryTimeout;
        }

        @Override
        public TermsEnum intersect(CompiledAutomaton compiled, BytesRef startTerm) throws IOException {
            return new ExitableTermsEnum(in.intersect(compiled, startTerm), queryTimeout);
        }

        @Override
        public TermsEnum iterator() throws IOException {
            return new ExitableTermsEnum(in.iterator(), queryTimeout);
        }
    }

    /**
     * Wrapper class for {@link FilterLeafReader.FilterTermsEnum} that is used by {@link ExitableTerms} for
     * implementing an exitable enumeration of terms.
     */
    private static class ExitableTermsEnum extends FilterLeafReader.FilterTermsEnum {

        private static final int MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK = (1 << 4) - 1; // 15

        private int calls;
        private final QueryTimeout queryTimeout;

        private ExitableTermsEnum(TermsEnum termsEnum, QueryTimeout queryTimeout) {
            super(termsEnum);
            this.queryTimeout = queryTimeout;
            this.queryTimeout.shouldExit();
        }

        private void checkAndThrowWithSampling() {
            if ((calls++ & MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK) == 0) {
                queryTimeout.shouldExit();
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
        private final QueryTimeout queryTimeout;

        private ExitablePointValues(PointValues in, QueryTimeout queryTimeout) {
            this.in = in;
            this.queryTimeout = queryTimeout;
            this.queryTimeout.shouldExit();
        }

        @Override
        public void intersect(IntersectVisitor visitor) throws IOException {
            queryTimeout.shouldExit();
            in.intersect(new ExitableIntersectVisitor(visitor, queryTimeout));
        }

        @Override
        public long estimatePointCount(IntersectVisitor visitor) {
            queryTimeout.shouldExit();
            return in.estimatePointCount(visitor);
        }

        @Override
        public byte[] getMinPackedValue() throws IOException {
            queryTimeout.shouldExit();
            return in.getMinPackedValue();
        }

        @Override
        public byte[] getMaxPackedValue() throws IOException {
            queryTimeout.shouldExit();
            return in.getMaxPackedValue();
        }

        @Override
        public int getNumDimensions() throws IOException {
            queryTimeout.shouldExit();
            return in.getNumDimensions();
        }

        @Override
        public int getNumIndexDimensions() throws IOException {
            queryTimeout.shouldExit();
            return in.getNumIndexDimensions();
        }

        @Override
        public int getBytesPerDimension() throws IOException {
            queryTimeout.shouldExit();
            return in.getBytesPerDimension();
        }

        @Override
        public long size() {
            queryTimeout.shouldExit();
            return in.size();
        }

        @Override
        public int getDocCount() {
            queryTimeout.shouldExit();
            return in.getDocCount();
        }
    }

    private static class ExitableIntersectVisitor implements PointValues.IntersectVisitor {

        private static final int MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK = (1 << 4) - 1; // 15

        private final PointValues.IntersectVisitor in;
        private final QueryTimeout queryTimeout;
        private int calls;

        private ExitableIntersectVisitor(PointValues.IntersectVisitor in, QueryTimeout queryTimeout) {
            this.in = in;
            this.queryTimeout = queryTimeout;
        }

        private void checkAndThrowWithSampling() {
            if ((calls++ & MAX_CALLS_BEFORE_QUERY_TIMEOUT_CHECK) == 0) {
                queryTimeout.shouldExit();
            }
        }

        @Override
        public void visit(int docID) throws IOException {
            checkAndThrowWithSampling();
            in.visit(docID);
        }

        @Override
        public void visit(int docID, byte[] packedValue) throws IOException {
            checkAndThrowWithSampling();
            in.visit(docID, packedValue);
        }

        @Override
        public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            queryTimeout.shouldExit();
            return in.compare(minPackedValue, maxPackedValue);
        }

        @Override
        public void grow(int count) {
            queryTimeout.shouldExit();
            in.grow(count);
        }
    }
}
