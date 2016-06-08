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
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import static org.apache.lucene.search.BooleanClause.Occur.FILTER;

public final class PercolateQuery extends Query implements Accountable {

    // cost of matching the query against the document, arbitrary as it would be really complex to estimate
    public static final float MATCH_COST = 1000;

    public static class Builder {

        private final String docType;
        private final QueryStore queryStore;
        private final BytesReference documentSource;
        private final IndexSearcher percolatorIndexSearcher;

        private Query queriesMetaDataQuery;
        private Query percolateTypeQuery;

        /**
         * @param docType                   The type of the document being percolated
         * @param queryStore                The lookup holding all the percolator queries as Lucene queries.
         * @param documentSource            The source of the document being percolated
         * @param percolatorIndexSearcher   The index searcher on top of the in-memory index that holds the document being percolated
         */
        public Builder(String docType, QueryStore queryStore, BytesReference documentSource, IndexSearcher percolatorIndexSearcher) {
            this.docType = Objects.requireNonNull(docType);
            this.documentSource = Objects.requireNonNull(documentSource);
            this.percolatorIndexSearcher = Objects.requireNonNull(percolatorIndexSearcher);
            this.queryStore = Objects.requireNonNull(queryStore);
        }

        /**
         * Optionally sets a query that reduces the number of queries to percolate based on extracted terms from
         * the document to be percolated.
         *
         * @param extractedTermsFieldName The name of the field to get the extracted terms from
         * @param unknownQueryFieldname The field used to mark documents whose queries couldn't all get extracted
         */
        public void extractQueryTermsQuery(String extractedTermsFieldName, String unknownQueryFieldname) throws IOException {
            this.queriesMetaDataQuery = ExtractQueryTermsService.createQueryTermsQuery(
                    percolatorIndexSearcher.getIndexReader(), extractedTermsFieldName, unknownQueryFieldname
            );
        }

        /**
         * @param percolateTypeQuery    A query that identifies all document containing percolator queries
         */
        public void setPercolateTypeQuery(Query percolateTypeQuery) {
            this.percolateTypeQuery = Objects.requireNonNull(percolateTypeQuery);
        }

        public PercolateQuery build() {
            if (percolateTypeQuery != null && queriesMetaDataQuery != null) {
                throw new IllegalStateException("Either filter by deprecated percolator type or by query metadata");
            }
            // The query that selects which percolator queries will be evaluated by MemoryIndex:
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            if (percolateTypeQuery != null) {
                builder.add(percolateTypeQuery, FILTER);
            }
            if (queriesMetaDataQuery != null) {
                builder.add(queriesMetaDataQuery, FILTER);
            }
            return new PercolateQuery(docType, queryStore, documentSource, builder.build(), percolatorIndexSearcher);
        }

    }

    private final String documentType;
    private final QueryStore queryStore;
    private final BytesReference documentSource;
    private final Query percolatorQueriesQuery;
    private final IndexSearcher percolatorIndexSearcher;

    private PercolateQuery(String documentType, QueryStore queryStore, BytesReference documentSource,
                           Query percolatorQueriesQuery, IndexSearcher percolatorIndexSearcher) {
        this.documentType = documentType;
        this.documentSource = documentSource;
        this.percolatorQueriesQuery = percolatorQueriesQuery;
        this.queryStore = queryStore;
        this.percolatorIndexSearcher = percolatorIndexSearcher;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query rewritten = percolatorQueriesQuery.rewrite(reader);
        if (rewritten != percolatorQueriesQuery) {
            return new PercolateQuery(documentType, queryStore, documentSource, rewritten, percolatorIndexSearcher);
        } else {
            return this;
        }
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        final Weight innerWeight = percolatorQueriesQuery.createWeight(searcher, needsScores);
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
                                QueryStore.Leaf percolatorQueries = queryStore.getQueries(leafReaderContext);
                                Query query = percolatorQueries.getQuery(docId);
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
            public float getValueForNormalization() throws IOException {
                return innerWeight.getValueForNormalization();
            }

            @Override
            public void normalize(float v, float v1) {
                innerWeight.normalize(v, v1);
            }

            @Override
            public Scorer scorer(LeafReaderContext leafReaderContext) throws IOException {
                final Scorer approximation = innerWeight.scorer(leafReaderContext);
                if (approximation == null) {
                    return null;
                }

                final QueryStore.Leaf queries = queryStore.getQueries(leafReaderContext);
                if (needsScores) {
                    return new BaseScorer(this, approximation, queries, percolatorIndexSearcher) {

                        float score;

                        @Override
                        boolean matchDocId(int docId) throws IOException {
                            Query query = percolatorQueries.getQuery(docId);
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
                    return new BaseScorer(this, approximation, queries, percolatorIndexSearcher) {

                        @Override
                        public float score() throws IOException {
                            return 0f;
                        }

                        boolean matchDocId(int docId) throws IOException {
                            Query query = percolatorQueries.getQuery(docId);
                            return query != null && Lucene.exists(percolatorIndexSearcher, query);
                        }
                    };
                }
            }
        };
    }

    public IndexSearcher getPercolatorIndexSearcher() {
        return percolatorIndexSearcher;
    }

    public String getDocumentType() {
        return documentType;
    }

    public BytesReference getDocumentSource() {
        return documentSource;
    }

    public QueryStore getQueryStore() {
        return queryStore;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (sameClassAs(o) == false) return false;

        PercolateQuery that = (PercolateQuery) o;

        if (!documentType.equals(that.documentType)) return false;
        return documentSource.equals(that.documentSource);

    }

    @Override
    public int hashCode() {
        int result = classHash();
        result = 31 * result + documentType.hashCode();
        result = 31 * result + documentSource.hashCode();
        return result;
    }

    @Override
    public String toString(String s) {
        return "PercolateQuery{document_type={" + documentType + "},document_source={" + documentSource.toUtf8() +
                "},inner={" + percolatorQueriesQuery.toString(s)  + "}}";
    }

    @Override
    public long ramBytesUsed() {
        long sizeInBytes = 0;
        if (documentSource.hasArray()) {
            sizeInBytes += documentSource.array().length;
        } else {
            sizeInBytes += documentSource.length();
        }
        return sizeInBytes;
    }

    @FunctionalInterface
    public interface QueryStore {

        Leaf getQueries(LeafReaderContext ctx) throws IOException;

        @FunctionalInterface
        interface Leaf {

            Query getQuery(int docId) throws IOException;

        }

    }

    static abstract class BaseScorer extends Scorer {

        final Scorer approximation;
        final QueryStore.Leaf percolatorQueries;
        final IndexSearcher percolatorIndexSearcher;

        BaseScorer(Weight weight, Scorer approximation, QueryStore.Leaf percolatorQueries, IndexSearcher percolatorIndexSearcher) {
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
