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

package org.elasticsearch.search.scan;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BitsFilteredDocIdSet;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.docset.AllDocIdSet;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentMap;

/**
 * The scan context allows to optimize readers we already processed during scanning. We do that by keeping track
 * of the count per reader, and if we are done with it, we no longer process it by using a filter that returns
 * null docIdSet for this reader.
 */
public class ScanContext {

    private final ConcurrentMap<IndexReader, ReaderState> readerStates = ConcurrentCollections.newConcurrentMap();

    public void clear() {
        readerStates.clear();
    }

    public TopDocs execute(SearchContext context) throws IOException {
        ScanCollector collector = new ScanCollector(readerStates, context.from(), context.size(), context.trackScores());
        Query query = new FilteredQuery(context.query(), new ScanFilter(readerStates, collector));
        try {
            context.searcher().search(query, collector);
        } catch (ScanCollector.StopCollectingException e) {
            // all is well
        }
        return collector.topDocs();
    }

    static class ScanCollector extends SimpleCollector {

        private final ConcurrentMap<IndexReader, ReaderState> readerStates;

        private final int from;

        private final int to;

        private final ArrayList<ScoreDoc> docs;

        private final boolean trackScores;

        private Scorer scorer;

        private int docBase;

        private int counter;

        private IndexReader currentReader;
        private ReaderState readerState;

        ScanCollector(ConcurrentMap<IndexReader, ReaderState> readerStates, int from, int size, boolean trackScores) {
            this.readerStates = readerStates;
            this.from = from;
            this.to = from + size;
            this.trackScores = trackScores;
            this.docs = new ArrayList<>(size);
        }

        void incCounter(int count) {
            this.counter += count;
        }

        public TopDocs topDocs() {
            return new TopDocs(docs.size(), docs.toArray(new ScoreDoc[docs.size()]), 0f);
        }

        @Override
        public boolean needsScores() {
            return trackScores;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
        }

        @Override
        public void collect(int doc) throws IOException {
            if (counter >= from) {
                docs.add(new ScoreDoc(docBase + doc, trackScores ? scorer.score() : 0f));
            }
            readerState.count++;
            counter++;
            if (counter >= to) {
                throw StopCollectingException;
            }
        }

        @Override
        public void doSetNextReader(LeafReaderContext context) throws IOException {
            // if we have a reader state, and we haven't registered one already, register it
            // we need to check in readersState since even when the filter return null, setNextReader is still
            // called for that reader (before)
            if (currentReader != null && !readerStates.containsKey(currentReader)) {
                assert readerState != null;
                readerState.done = true;
                readerStates.put(currentReader, readerState);
            }
            this.currentReader = context.reader();
            this.docBase = context.docBase;
            this.readerState = new ReaderState();
        }

        public static final RuntimeException StopCollectingException = new StopCollectingException();

        static class StopCollectingException extends RuntimeException {
            @Override
            public Throwable fillInStackTrace() {
                return null;
            }
        }
    }

    public static class ScanFilter extends Filter {

        private final ConcurrentMap<IndexReader, ReaderState> readerStates;

        private final ScanCollector scanCollector;

        public ScanFilter(ConcurrentMap<IndexReader, ReaderState> readerStates, ScanCollector scanCollector) {
            this.readerStates = readerStates;
            this.scanCollector = scanCollector;
        }

        @Override
        public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptedDocs) throws IOException {
            ReaderState readerState = readerStates.get(context.reader());
            if (readerState != null && readerState.done) {
                scanCollector.incCounter(readerState.count);
                return null;
            }
            return BitsFilteredDocIdSet.wrap(new AllDocIdSet(context.reader().maxDoc()), acceptedDocs);
        }

        @Override
        public String toString(String field) {
            return "ScanFilter";
        }
    }

    static class ReaderState {
        public int count;
        public boolean done;
    }
}
