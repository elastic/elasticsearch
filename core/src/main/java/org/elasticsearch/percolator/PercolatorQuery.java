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
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.Uid;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

public final class PercolatorQuery extends Query {

    private final Query percolatorQueriesQuery;
    // TODO: should we use stored fields instead?
    // The two phase execution makes really lookup the query id when we need to percolator.
    // Percolating is heavy so lookup stored fields shouldn't be a big of a deal?
    // (_uid field is dense, so heavy to keep in memory)
    private final IndexFieldData<?> uidFieldData;
    private final IndexSearcher percolatorIndexSearcher;
    private final boolean percolatorIndexSearcherHoldsNestedDocs;
    private final ConcurrentMap<BytesRef, Query> percolatorQueries;
    private final Lucene.EarlyTerminatingCollector collector = Lucene.createExistsCollector();

    public PercolatorQuery(Query percolatorQueriesQuery, IndexFieldData<?> uidFieldData, IndexSearcher percolatorIndexSearcher, boolean percolatorIndexSearcherHoldsNestedDocs, ConcurrentMap<BytesRef, Query> percolatorQueries) {
        this.percolatorQueriesQuery = percolatorQueriesQuery;
        this.uidFieldData = uidFieldData;
        this.percolatorIndexSearcher = percolatorIndexSearcher;
        this.percolatorIndexSearcherHoldsNestedDocs = percolatorIndexSearcherHoldsNestedDocs;
        this.percolatorQueries = percolatorQueries;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        if (getBoost() != 1f) {
            return super.rewrite(reader);
        }

        Query rewritten = percolatorQueriesQuery.rewrite(reader);
        if (rewritten != percolatorQueriesQuery) {
            return new PercolatorQuery(rewritten, uidFieldData, percolatorIndexSearcher, percolatorIndexSearcherHoldsNestedDocs, percolatorQueries);
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
            public Explanation explain(LeafReaderContext leafReaderContext, int i) throws IOException {
                return null;
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

                final SortedBinaryDocValues values = uidFieldData.load(leafReaderContext).getBytesValues();
                return new Scorer(this) {

                    @Override
                    public TwoPhaseIterator asTwoPhaseIterator() {
                        return new TwoPhaseIterator(approximation) {
                            @Override
                            public boolean matches() throws IOException {
                                return matchDocId(approximation.docID(), values);
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

                    @Override
                    public int nextDoc() throws IOException {
                        return advance(approximation.docID() + 1);
                    }

                    @Override
                    public int advance(int target) throws IOException {
                        for (int docId = approximation.advance(target); docId < NO_MORE_DOCS; docId = approximation.advance(docId + 1)) {
                            if (matchDocId(docId, values)) {
                                return docId;
                            }
                        }
                        return NO_MORE_DOCS;
                    }

                    @Override
                    public long cost() {
                        return approximation.cost();
                    }
                };
            }
        };
    }

    boolean matchDocId(int docId, SortedBinaryDocValues values) throws IOException {
        values.setDocument(docId);
        final int numValues = values.count();
        if (numValues == 0) {
            return false;
        }
        assert numValues == 1;
        BytesRef percolatorQueryId = Uid.splitUidIntoTypeAndId(values.valueAt(0))[1];
        Query percolatorQuery = percolatorQueries.get(percolatorQueryId);
        if (percolatorIndexSearcherHoldsNestedDocs) {
            Lucene.exists(percolatorIndexSearcher, percolatorQuery, Queries.newNonNestedFilter(), collector);
        } else {
            Lucene.exists(percolatorIndexSearcher, percolatorQuery, collector);
        }
        return collector.exists();
    }

    @Override
    public String toString(String s) {
        return "PercolatorQuery{inner={" + percolatorQueriesQuery.toString(s)  + "}}";
    }
}
