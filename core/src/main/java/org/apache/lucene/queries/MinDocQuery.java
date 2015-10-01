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

package org.apache.lucene.queries;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;

/** A {@link Query} that only matches documents that are greater than or equal
 *  to a configured doc ID. */
public final class MinDocQuery extends Query {

    private final int minDoc;

    /** Sole constructor. */
    public MinDocQuery(int minDoc) {
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
            public Scorer scorer(LeafReaderContext context) throws IOException {
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
                        if (doc >= maxDoc) {
                            doc = NO_MORE_DOCS;
                        }
                        return doc;
                    }

                    @Override
                    public long cost() {
                        return maxDoc - segmentMinDoc;
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
