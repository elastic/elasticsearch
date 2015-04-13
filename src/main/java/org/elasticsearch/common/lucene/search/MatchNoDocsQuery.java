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

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Set;

/**
 * Query that matches no documents.
 */
public final class MatchNoDocsQuery extends Query {

    /**
     * Weight implementation that matches no documents.
     */
    private class MatchNoDocsWeight extends Weight {

        MatchNoDocsWeight(Query parent) {
            super(parent);
        }

        @Override
        public String toString() {
            return "weight(" + MatchNoDocsQuery.this + ")";
        }

        @Override
        public float getValueForNormalization() throws IOException {
            return 0;
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
        }

        @Override
        public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
            return null;
        }

        @Override
        public Explanation explain(final LeafReaderContext context,
                                   final int doc) {
            return new ComplexExplanation(false, 0, "MatchNoDocs matches nothing");
        }
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        return new MatchNoDocsWeight(this);
    }

    @Override
    public void extractTerms(final Set<Term> terms) {
    }

    @Override
    public String toString(final String field) {
        return "MatchNoDocsQuery";
    }

    @Override
    public boolean equals(final Object o) {
        if (o instanceof MatchNoDocsQuery) {
            return getBoost() == ((MatchNoDocsQuery) o).getBoost();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return getClass().hashCode() ^ Float.floatToIntBits(getBoost());
    }
}