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

package org.apache.lucene.search.suggest.analyzing;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Simple Lookup interface for {@link CharSequence} suggestions.
 * @lucene.experimental
 */
public abstract class SegmentLookup<W> implements Accountable {

    /**
     * Sole constructor. (For invocation by subclass
     * constructors, typically implicit.)
     */
    public SegmentLookup() {
    }


    /**
     * Collects Lookup results
     * @param <W> type of score
     */
    public static interface Collector<W> {

        /**
         * Called every time an analyzed form is collected in order specified by the configured scorer
         *
         * @param key the matched analyzed form
         * @param results a list of output, score and docID of the matched documents
         * @throws IOException
         */
        void collect(CharSequence key, List<ResultMetaData<W>> results) throws IOException;

        public static final class ResultMetaData<W> {
            private final CharSequence outputForm;
            private final W score;
            private final int docId;

            public ResultMetaData(CharSequence outputForm, W score, int docId) {
                this.outputForm = outputForm;
                this.score = score;
                this.docId = docId;
            }

            public CharSequence output() {
                return outputForm;
            }

            public W score() {
                return score;
            }
            public int docId() {
                return docId;
            }
        }
    }


    /**
     * Context holds a <code>filter</code> to filter docIds by
     * and <code>scoreContext</code> for context aware lookup
     * @param <W> score type
     */
    public static class Context<W> {

        final W scoreContext;
        final Filter filter;

        public static interface Filter {
            Bits bits();
            int size();
            int cardinality();
        }

        /**
         * Context with only context aware score
         */
        public Context(W scoreContext) {
            this(scoreContext, null);
        }

        /**
         * Context with only context filter
         */
        public Context(Filter filter) {
            this(null, filter);
        }

        /**
         * Context with <code>scoreContext</code> and <code>filter</code>
         */
        public Context(W scoreContext, Filter filter) {
            this.scoreContext = scoreContext;
            this.filter = filter;
        }
    }

    /**
     * Calls {@link #lookup(LeafReader, Analyzer, CharSequence, int, int, Context, Collector)}
     * with no context
     */
    public void lookup(final LeafReader reader, final Analyzer queryAnalyzer, final CharSequence key, int num, int nLeaf, final Collector<W> collector) {
        lookup(reader, queryAnalyzer, key, num, nLeaf, null, collector);
    }

    /**
     * Lookup a <code>key</code> and collect possible completion entries
     *
     * @param reader to be used for filtering deleted docs
     * @param queryAnalyzer analyzer to be applied to provided key
     * @param key prefix to lookup
     * @param num number of unique analyzed forms
     * @param nLeaf number of documents to be retrieved per analyzed form
     * @param context query context and filter context for the lookup
     * @param collector used to collect the results of the lookup
     */
    public abstract void lookup(final LeafReader reader, final Analyzer queryAnalyzer, final CharSequence key, int num, int nLeaf, Context<W> context, final Collector<W> collector);


    /**
     * SegmentLookup wrapper base for Long and BytesRef based scores
     * @param <W> score type
     */
    public static abstract class SegmentLookupWrapper<W> extends SegmentLookup<W> {
        private final SegmentLookup<W> segmentLookup;
        private final Analyzer queryAnalyzer;


        /**
         * @param segmentLookup implementation
         * @param queryAnalyzer analyzer used to tokenize lookup key
         */
        public SegmentLookupWrapper(SegmentLookup<W> segmentLookup, Analyzer queryAnalyzer) {
            this.segmentLookup = segmentLookup;
            this.queryAnalyzer = queryAnalyzer;
        }

        @Override
        public long ramBytesUsed() {
            return segmentLookup.ramBytesUsed();
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }

        /**
         * Calls {@link #lookup(LeafReader, Analyzer, CharSequence, int, int, Context, Collector)} with <code>queryAnalyzer</code>
         */
        public void lookup(LeafReader reader, CharSequence key, int num, int nLeaf, Context<W> context, Collector<W> collector) {
            segmentLookup.lookup(reader, queryAnalyzer, key, num, nLeaf, context, collector);
        }

        @Override
        public void lookup(LeafReader reader, Analyzer queryAnalyzer, CharSequence key, int num, int nLeaf, Context<W> context, Collector<W> collector) {
            segmentLookup.lookup(reader, queryAnalyzer, key, num, nLeaf, context, collector);
        }
    }

    /**
     * SegmentLookup wrapper for long score based lookup
     */
    public static class LongBased extends SegmentLookupWrapper<Long> {

        /**
         * Calls {@link SegmentLookupWrapper}
         */
        public LongBased(SegmentLookup<Long> segmentLookup, Analyzer queryAnalyzer) {
            super(segmentLookup, queryAnalyzer);
        }

        /**
         * long score based lookup with {@link DefaultCollector}
         */
        public List<DefaultCollector.Result<Long>> lookup(LeafReader reader, CharSequence key, int num, int nLeaf, Context<Long> context) {
            DefaultCollector<Long> defaultLongCollector = new DefaultCollector<>();
            this.lookup(reader, key, num, nLeaf, context, defaultLongCollector);
            return defaultLongCollector.get();
        }

        /**
         * long score based lookup with {@link DefaultCollector} and no context
         */
        public List<DefaultCollector.Result<Long>> lookup(LeafReader reader, CharSequence key, int num, int nLeaf) {
            return lookup(reader, key, num, nLeaf, null);
        }

        /**
         * long score based lookup with {@link DefaultCollector} and no context and <code>nLeaf = 1</code>
         */
        public List<DefaultCollector.Result<Long>> lookup(LeafReader reader, CharSequence key, int num) {
            return lookup(reader, key, num, 1);
        }
    }

    public static class BytesBased extends SegmentLookupWrapper<BytesRef> {

        public BytesBased(SegmentLookup<BytesRef> segmentLookup, Analyzer queryAnalyzer) {
            super(segmentLookup, queryAnalyzer);
        }
    }

}

