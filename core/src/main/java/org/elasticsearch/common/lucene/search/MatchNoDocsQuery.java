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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Scorer;

import java.io.IOException;
import java.util.Set;

/**
 * A query that matches no documents and prints the reason why in the toString method.
 */
public class MatchNoDocsQuery extends Query {
    /**
     * The reason why the query does not match any document.
     */
    private final String reason;

    public MatchNoDocsQuery(String reason) {
        this.reason = reason;
    }

    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        return new ConstantScoreWeight(this) {
            @Override
            public void extractTerms(Set<Term> terms) {
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                return Explanation.noMatch(reason);
            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                return null;
            }
        };
    }

    @Override
    public String toString(String field) {
        return "MatchNoDocsQuery[\"" + reason + "\"]";
    }

    @Override
    public boolean equals(Object obj) {
        return sameClassAs(obj);
    }

    @Override
    public int hashCode() {
        return classHash();
    }
}
