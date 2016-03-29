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

package org.elasticsearch.index.query;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.percolator.ExtractQueryTermsService;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import static org.apache.lucene.search.BooleanClause.Occur.FILTER;
import static org.apache.lucene.search.BooleanClause.Occur.MUST;

public final class PercolatorQuery extends Query implements Accountable {

    // cost of matching the query against the document, arbitrary as it would be really complex to estimate
    public static final float MATCH_COST = 1000;

    public static class Builder {

        private final String docType;
        private final QueryRegistry queryRegistry;
        private final BytesReference documentSource;
        private final IndexSearcher percolatorIndexSearcher;

        private Query queriesMetaDataQuery;
        private final Query percolateTypeQuery;

        /**
         * @param docType                   The type of the document being percolated
         * @param queryRegistry             The registry holding all the percolator queries as Lucene queries.
         * @param documentSource            The source of the document being percolated
         * @param percolatorIndexSearcher   The index searcher on top of the in-memory index that holds the document being percolated
         * @param percolateTypeQuery        A query that identifies all document containing percolator queries
         */
        public Builder(String docType, QueryRegistry queryRegistry, BytesReference documentSource, IndexSearcher percolatorIndexSearcher,
                Query percolateTypeQuery) {
            this.docType = Objects.requireNonNull(docType);
            this.documentSource = Objects.requireNonNull(documentSource);
            this.percolatorIndexSearcher = Objects.requireNonNull(percolatorIndexSearcher);
            this.queryRegistry = Objects.requireNonNull(queryRegistry);
            this.percolateTypeQuery = Objects.requireNonNull(percolateTypeQuery);
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

        public PercolatorQuery build() {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(percolateTypeQuery, FILTER);
            if (queriesMetaDataQuery != null) {
                builder.add(queriesMetaDataQuery, FILTER);
            }
            return new PercolatorQuery(docType, queryRegistry, documentSource, builder.build(), percolatorIndexSearcher);
        }

    }

    private final String documentType;
    private final QueryRegistry queryRegistry;
    private final BytesReference documentSource;
    private final Query percolatorQueriesQuery;
    private final IndexSearcher percolatorIndexSearcher;

    private PercolatorQuery(String documentType, QueryRegistry queryRegistry, BytesReference documentSource,
                            Query percolatorQueriesQuery, IndexSearcher percolatorIndexSearcher) {
        this.documentType = documentType;
        this.documentSource = documentSource;
        this.percolatorQueriesQuery = percolatorQueriesQuery;
        this.queryRegistry = queryRegistry;
        this.percolatorIndexSearcher = percolatorIndexSearcher;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query rewritten = percolatorQueriesQuery.rewrite(reader);
        if (rewritten != percolatorQueriesQuery) {
            return new PercolatorQuery(documentType, queryRegistry, documentSource, rewritten, percolatorIndexSearcher);
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
                            return Explanation.match(scorer.score(), "PercolatorQuery");
                        }
                    }
                }
                return Explanation.noMatch("PercolatorQuery");
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

                final QueryRegistry.Leaf percolatorQueries = queryRegistry.getQueries(leafReaderContext);
                return new Scorer(this) {

                    @Override
                    public DocIdSetIterator iterator() {
                        return TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator());
                    }

                    @Override
                    public TwoPhaseIterator twoPhaseIterator() {
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
                    public float score() throws IOException {
                        return approximation.score();
                    }

                    @Override
                    public int freq() throws IOException {
                        return approximation.freq();
                    }

                    @Override
                    public int docID() {
                        return approximation.docID();
                    }

                    boolean matchDocId(int docId) throws IOException {
                        Query query = percolatorQueries.getQuery(docId);
                        if (query != null) {
                            return Lucene.exists(percolatorIndexSearcher, query);
                        } else {
                            return false;
                        }
                    }
                };
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        PercolatorQuery that = (PercolatorQuery) o;

        if (!documentType.equals(that.documentType)) return false;
        return documentSource.equals(that.documentSource);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + documentType.hashCode();
        result = 31 * result + documentSource.hashCode();
        return result;
    }

    @Override
    public String toString(String s) {
        return "PercolatorQuery{document_type={" + documentType + "},document_source={" + documentSource.toUtf8() +
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

    public interface QueryRegistry {

        Leaf getQueries(LeafReaderContext ctx);

        interface Leaf {

            Query getQuery(int docId);

        }

    }

}
