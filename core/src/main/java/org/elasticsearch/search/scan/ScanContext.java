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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The scan context allows to optimize readers we already processed during scanning. We do that by keeping track
 * of the last collected doc ID and only collecting doc IDs that are greater.
 */
public class ScanContext {

    private volatile int docUpTo;

    public TopDocs execute(SearchContext context) throws IOException {
        return execute(context.searcher(), context.query(), context.size(), context.trackScores());
    }

    TopDocs execute(IndexSearcher searcher, Query query, int size, boolean trackScores) throws IOException {
        ScanCollector collector = new ScanCollector(size, trackScores);
        Query q = Queries.filtered(query, new MinDocQuery(docUpTo));
        searcher.search(q, collector);
        return collector.topDocs();
    }

    private class ScanCollector extends SimpleCollector {

        private final List<ScoreDoc> docs;

        private final int size;

        private final boolean trackScores;

        private Scorer scorer;

        private int docBase;

        ScanCollector(int size, boolean trackScores) {
            this.trackScores = trackScores;
            this.docs = new ArrayList<>(size);
            this.size = size;
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
            int topLevelDoc = docBase + doc;
            docs.add(new ScoreDoc(topLevelDoc, trackScores ? scorer.score() : 0f));
            // record that we collected up to this document
            assert topLevelDoc >= docUpTo;
            docUpTo = topLevelDoc + 1;
            if (docs.size() >= size) {
                throw new CollectionTerminatedException();
            }
        }

        @Override
        public void doSetNextReader(LeafReaderContext context) throws IOException {
            if (docs.size() >= size || context.docBase + context.reader().maxDoc() <= docUpTo) {
                // no need to collect a new segment, we either already collected enough
                // or the segment is not competitive
                throw new CollectionTerminatedException();
            }
            docBase = context.docBase;
        }
    }

    /**
     * A filtering query that matches all doc IDs that are not deleted and
     * greater than or equal to the configured doc ID.
     */
    // pkg-private for testing
    static class MinDocQuery extends Query {

        private final int minDoc;

        MinDocQuery(int minDoc) {
            this.minDoc = minDoc;
        }

        @Override
        public int hashCode() {
            return 31 * super.hashCode() + minDoc;
        }

        @Override
        public boolean equals(Object obj) {
            if (super.equals(obj) == false) {
                return false;
            }
            MinDocQuery that = (MinDocQuery) obj;
            return minDoc == that.minDoc;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
            return new ConstantScoreWeight(this) {
                @Override
                public Scorer scorer(LeafReaderContext context, final Bits acceptDocs) throws IOException {
                    final int maxDoc = context.reader().maxDoc();
                    if (context.docBase + maxDoc <= minDoc) {
                        return null;
                    }
                    final int segmentMinDoc = Math.max(0, minDoc - context.docBase);
                    final DocIdSetIterator disi = new DocIdSetIterator() {

                        int doc = -1;

                        @Override
                        public int docID() {
                            return doc;
                        }

                        @Override
                        public int nextDoc() throws IOException {
                            return advance(doc + 1);
                        }

                        @Override
                        public int advance(int target) throws IOException {
                            assert target > doc;
                            if (doc == -1) {
                                // skip directly to minDoc
                                doc = Math.max(target, segmentMinDoc);
                            } else {
                                doc = target;
                            }
                            while (doc < maxDoc) {
                                if (acceptDocs == null || acceptDocs.get(doc)) {
                                    break;
                                }
                                doc += 1;
                            }
                            if (doc >= maxDoc) {
                                doc = NO_MORE_DOCS;
                            }
                            return doc;
                        }

                        @Override
                        public long cost() {
                            return maxDoc - minDoc;
                        }

                    };
                    return new ConstantScoreScorer(this, score(), disi);
                }
            };
        }

        @Override
        public String toString(String field) {
            return "MinDocQuery(minDoc=" + minDoc  + ")";
        }

    }
}
