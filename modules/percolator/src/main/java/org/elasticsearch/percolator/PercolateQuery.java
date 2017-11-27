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

package org.elasticsearch.percolator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;

final class PercolateQuery extends Query implements Accountable {

    // cost of matching the query against the document, arbitrary as it would be really complex to estimate
    private static final float MATCH_COST = 1000;

    private final String name;
    private final QueryStore queryStore;
    private final List<BytesReference> documents;
    private final Query candidateMatchesQuery;
    private final Query verifiedMatchesQuery;
    private final IndexSearcher percolatorIndexSearcher;

    PercolateQuery(String name, QueryStore queryStore, List<BytesReference> documents,
                   Query candidateMatchesQuery, IndexSearcher percolatorIndexSearcher, Query verifiedMatchesQuery) {
        this.name = name;
        this.documents = Objects.requireNonNull(documents);
        this.candidateMatchesQuery = Objects.requireNonNull(candidateMatchesQuery);
        this.queryStore = Objects.requireNonNull(queryStore);
        this.percolatorIndexSearcher = Objects.requireNonNull(percolatorIndexSearcher);
        this.verifiedMatchesQuery = Objects.requireNonNull(verifiedMatchesQuery);
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query rewritten = candidateMatchesQuery.rewrite(reader);
        if (rewritten != candidateMatchesQuery) {
            return new PercolateQuery(name, queryStore, documents, rewritten, percolatorIndexSearcher, verifiedMatchesQuery);
        } else {
            return this;
        }
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
        final Weight verifiedMatchesWeight = verifiedMatchesQuery.createWeight(searcher, false, boost);
        final Weight candidateMatchesWeight = candidateMatchesQuery.createWeight(searcher, false, boost);
        return new Weight(this) {
            @Override
            public void extractTerms(Set<Term> set) {
            }

            @Override
            public Explanation explain(LeafReaderContext leafReaderContext, int docId) throws IOException {
                Scorer scorer = scorer(leafReaderContext);
                if (scorer != null) {
                    TwoPhaseIterator twoPhaseIterator = scorer.twoPhaseIterator();
                    int result = twoPhaseIterator.approximation().advance(docId);
                    if (result == docId) {
                        if (twoPhaseIterator.matches()) {
                            if (needsScores) {
                                CheckedFunction<Integer, Query, IOException> percolatorQueries = queryStore.getQueries(leafReaderContext);
                                Query query = percolatorQueries.apply(docId);
                                Explanation detail = percolatorIndexSearcher.explain(query, 0);
                                return Explanation.match(scorer.score(), "PercolateQuery", detail);
                            } else {
                                return Explanation.match(scorer.score(), "PercolateQuery");
                            }
                        }
                    }
                }
                return Explanation.noMatch("PercolateQuery");
            }

            @Override
            public Scorer scorer(LeafReaderContext leafReaderContext) throws IOException {
                final Scorer approximation = candidateMatchesWeight.scorer(leafReaderContext);
                if (approximation == null) {
                    return null;
                }

                final CheckedFunction<Integer, Query, IOException> queries = queryStore.getQueries(leafReaderContext);
                if (needsScores) {
                    return new BaseScorer(this, approximation, queries, percolatorIndexSearcher) {

                        float score;

                        @Override
                        boolean matchDocId(int docId) throws IOException {
                            Query query = percolatorQueries.apply(docId);
                            if (query != null) {
                                TopDocs topDocs = percolatorIndexSearcher.search(query, 1);
                                if (topDocs.totalHits > 0) {
                                    score = topDocs.scoreDocs[0].score;
                                    return true;
                                } else {
                                    return false;
                                }
                            } else {
                                return false;
                            }
                        }

                        @Override
                        public float score() throws IOException {
                            return score;
                        }
                    };
                } else {
                    ScorerSupplier verifiedDocsScorer = verifiedMatchesWeight.scorerSupplier(leafReaderContext);
                    Bits verifiedDocsBits = Lucene.asSequentialAccessBits(leafReaderContext.reader().maxDoc(), verifiedDocsScorer);
                    return new BaseScorer(this, approximation, queries, percolatorIndexSearcher) {

                        @Override
                        public float score() throws IOException {
                            return 0f;
                        }

                        boolean matchDocId(int docId) throws IOException {
                            // We use the verifiedDocsBits to skip the expensive MemoryIndex verification.
                            // If docId also appears in the verifiedDocsBits then that means during indexing
                            // we were able to extract all query terms and for this candidate match
                            // and we determined based on the nature of the query that it is safe to skip
                            // the MemoryIndex verification.
                            if (verifiedDocsBits.get(docId)) {
                                return true;
                            }
                            Query query = percolatorQueries.apply(docId);
                            return query != null && Lucene.exists(percolatorIndexSearcher, query);
                        }
                    };
                }
            }
        };
    }

    String getName() {
        return name;
    }

    IndexSearcher getPercolatorIndexSearcher() {
        return percolatorIndexSearcher;
    }

    List<BytesReference> getDocuments() {
        return documents;
    }

    QueryStore getQueryStore() {
        return queryStore;
    }

    Query getCandidateMatchesQuery() {
        return candidateMatchesQuery;
    }

    // Comparing identity here to avoid being cached
    // Note that in theory if the same instance gets used multiple times it could still get cached,
    // however since we create a new query instance each time we this query this shouldn't happen and thus
    // this risk neglectable.
    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    // Computing hashcode based on identity to avoid caching.
    @Override
    public int hashCode() {
        return System.identityHashCode(this);
    }

    @Override
    public String toString(String s) {
        StringBuilder sources = new StringBuilder();
        for (BytesReference document : documents) {
            sources.append(document.utf8ToString());
            sources.append('\n');
        }
        return "PercolateQuery{document_sources={" + sources + "},inner={" +
            candidateMatchesQuery.toString(s)  + "}}";
    }

    @Override
    public long ramBytesUsed() {
        long ramUsed = 0L;
        for (BytesReference document : documents) {
            ramUsed += document.ramBytesUsed();
        }
        return ramUsed;
    }

    @FunctionalInterface
    interface QueryStore {
        CheckedFunction<Integer, Query, IOException> getQueries(LeafReaderContext ctx) throws IOException;
    }

    abstract static class BaseScorer extends Scorer {

        final Scorer approximation;
        final CheckedFunction<Integer, Query, IOException> percolatorQueries;
        final IndexSearcher percolatorIndexSearcher;

        BaseScorer(Weight weight, Scorer approximation, CheckedFunction<Integer, Query, IOException> percolatorQueries,
                   IndexSearcher percolatorIndexSearcher) {
            super(weight);
            this.approximation = approximation;
            this.percolatorQueries = percolatorQueries;
            this.percolatorIndexSearcher = percolatorIndexSearcher;
        }

        @Override
        public final DocIdSetIterator iterator() {
            return TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator());
        }

        @Override
        public final TwoPhaseIterator twoPhaseIterator() {
            return new TwoPhaseIterator(approximation.iterator()) {
                @Override
                public boolean matches() throws IOException {
                    return matchDocId(approximation.docID());
                }

                @Override
                public float matchCost() {
                    return MATCH_COST;
                }
            };
        }

        @Override
        public final int freq() throws IOException {
            return approximation.freq();
        }

        @Override
        public final int docID() {
            return approximation.docID();
        }

        abstract boolean matchDocId(int docId) throws IOException;

    }

}
