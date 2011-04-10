/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.apache.lucene.search;

import org.apache.lucene.index.IndexReader;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
// LUCENE MONITOR: Against ConstantScoreQuery, basically added logic in the doc iterator to take deletions into account
// So it can basically be cached safely even with a reader that changes deletions but remain with teh same cache key
// See more: https://issues.apache.org/jira/browse/LUCENE-2468
public class DeletionAwareConstantScoreQuery extends ConstantScoreQuery {

    public DeletionAwareConstantScoreQuery(Filter filter) {
        super(filter);
    }

    @Override
    public Weight createWeight(Searcher searcher) throws IOException {
        return new DeletionConstantWeight(searcher);
    }

    protected class DeletionConstantWeight extends ConstantWeight {

        private final Similarity similarity;

        public DeletionConstantWeight(Searcher searcher) throws IOException {
            super(searcher);
            similarity = getSimilarity(searcher);
        }

        @Override public Scorer scorer(IndexReader reader, boolean scoreDocsInOrder, boolean topScorer) throws IOException {
            final DocIdSet dis = filter.getDocIdSet(reader);
            if (dis == null)
                return null;
            DocIdSetIterator disi = dis.iterator();
            if (disi == null) {
                return null;
            }
            return new DeletionConstantScorer(reader, similarity, disi, this);
        }
    }

    protected class DeletionConstantScorer extends ConstantScorer {

        private final IndexReader reader;
        private int doc = -1;

        public DeletionConstantScorer(IndexReader reader, Similarity similarity, DocIdSetIterator docIdSetIterator, Weight w) throws IOException {
            super(similarity, docIdSetIterator, w);
            this.reader = reader;
        }

        @Override
        public int nextDoc() throws IOException {
            while ((doc = docIdSetIterator.nextDoc()) != NO_MORE_DOCS) {
                if (!reader.isDeleted(doc)) {
                    return doc;
                }
            }
            return doc;
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int advance(int target) throws IOException {
            doc = docIdSetIterator.advance(target);
            if (doc != NO_MORE_DOCS) {
                if (!reader.isDeleted(doc)) {
                    return doc;
                } else {
                    while ((doc = docIdSetIterator.nextDoc()) != NO_MORE_DOCS) {
                        if (!reader.isDeleted(doc)) {
                            return doc;
                        }
                    }
                    return doc;
                }
            }
            return doc;
        }
    }
}

