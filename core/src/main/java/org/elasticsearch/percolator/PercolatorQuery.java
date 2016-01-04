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
import org.apache.lucene.index.LeafReader;
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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.fieldvisitor.SingleFieldsVisitor;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.percolator.ExtractQueryTermsService;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.apache.lucene.search.BooleanClause.Occur.FILTER;
import static org.apache.lucene.search.BooleanClause.Occur.MUST;

final class PercolatorQuery extends Query {

    public static final float MATCH_COST =
            (1 << 14) // stored field access cost, approximated by the number of bytes in a block
            + 1000;   // cost of matching the query against the document, arbitrary as it would be really complex to estimate

    static class Builder {

        private final IndexSearcher percolatorIndexSearcher;
        private final Map<BytesRef, Query> percolatorQueries;

        private Query percolateQuery;
        private Query queriesMetaDataQuery;
        private final Query percolateTypeQuery;

        /**
         * @param percolatorIndexSearcher The index searcher on top of the in-memory index that holds the document being percolated
         * @param percolatorQueries All the registered percolator queries
         * @param percolateTypeQuery A query that identifies all document containing percolator queries
         */
        Builder(IndexSearcher percolatorIndexSearcher, Map<BytesRef, Query> percolatorQueries, Query percolateTypeQuery) {
            this.percolatorIndexSearcher = percolatorIndexSearcher;
            this.percolatorQueries = percolatorQueries;
            this.percolateTypeQuery = percolateTypeQuery;
        }

        /**
         * Optionally sets a query that reduces the number of queries to percolate based on custom metadata attached
         * on the percolator documents.
         */
        void setPercolateQuery(Query percolateQuery) {
            this.percolateQuery = percolateQuery;
        }

        /**
         * Optionally sets a query that reduces the number of queries to percolate based on extracted terms from
         * the document to be percolated.
         *
         * @param extractedTermsFieldName The name of the field to get the extracted terms from
         * @param unknownQueryFieldname The field used to mark documents whose queries couldn't all get extracted
         */
        void extractQueryTermsQuery(String extractedTermsFieldName, String unknownQueryFieldname) throws IOException {
            this.queriesMetaDataQuery = ExtractQueryTermsService.createQueryTermsQuery(percolatorIndexSearcher.getIndexReader(), extractedTermsFieldName, unknownQueryFieldname);
        }

        PercolatorQuery build() {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(percolateTypeQuery, FILTER);
            if (queriesMetaDataQuery != null) {
                builder.add(queriesMetaDataQuery, FILTER);
            }
            if (percolateQuery != null){
                builder.add(percolateQuery, MUST);
            }
            return new PercolatorQuery(builder.build(), percolatorIndexSearcher, percolatorQueries);
        }

    }

    private final Query percolatorQueriesQuery;
    private final IndexSearcher percolatorIndexSearcher;
    private final Map<BytesRef, Query> percolatorQueries;

    private PercolatorQuery(Query percolatorQueriesQuery, IndexSearcher percolatorIndexSearcher, Map<BytesRef, Query> percolatorQueries) {
        this.percolatorQueriesQuery = percolatorQueriesQuery;
        this.percolatorIndexSearcher = percolatorIndexSearcher;
        this.percolatorQueries = percolatorQueries;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        if (getBoost() != 1f) {
            return super.rewrite(reader);
        }

        Query rewritten = percolatorQueriesQuery.rewrite(reader);
        if (rewritten != percolatorQueriesQuery) {
            return new PercolatorQuery(rewritten, percolatorIndexSearcher, percolatorQueries);
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
                    int result = scorer.iterator().advance(docId);
                    if (result == docId) {
                        return Explanation.match(scorer.score(), "PercolatorQuery");
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

                final LeafReader leafReader = leafReaderContext.reader();
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
                                return matchDocId(approximation.docID(), leafReader);
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

                    boolean matchDocId(int docId, LeafReader leafReader) throws IOException {
                        SingleFieldsVisitor singleFieldsVisitor = new SingleFieldsVisitor(UidFieldMapper.NAME);
                        leafReader.document(docId, singleFieldsVisitor);
                        BytesRef percolatorQueryId = new BytesRef(singleFieldsVisitor.uid().id());
                        return matchQuery(percolatorQueryId);
                    }
                };
            }
        };
    }

    boolean matchQuery(BytesRef percolatorQueryId) throws IOException {
        Query percolatorQuery = percolatorQueries.get(percolatorQueryId);
        if (percolatorQuery != null) {
            return Lucene.exists(percolatorIndexSearcher, percolatorQuery);
        } else {
            return false;
        }
    }

    private final Object instance = new Object();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        PercolatorQuery that = (PercolatorQuery) o;

        return instance.equals(that.instance);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + instance.hashCode();
        return result;
    }

    @Override
    public String toString(String s) {
        return "PercolatorQuery{inner={" + percolatorQueriesQuery.toString(s)  + "}}";
    }
}
