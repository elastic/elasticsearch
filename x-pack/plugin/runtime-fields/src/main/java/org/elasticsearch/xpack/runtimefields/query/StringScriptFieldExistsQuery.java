/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;

import java.io.IOException;
import java.util.Objects;

public class StringScriptFieldExistsQuery extends Query {
    private final StringScriptFieldScript.LeafFactory leafFactory;
    private final String fieldName;

    public StringScriptFieldExistsQuery(StringScriptFieldScript.LeafFactory leafFactory, String fieldName) {
        this.leafFactory = leafFactory;
        this.fieldName = fieldName;
    }

    @Override
    public final Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {
            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false; // scripts aren't really cacheable at this point
            }

            @Override
            public Scorer scorer(LeafReaderContext ctx) throws IOException {
                StringScriptFieldScript script = leafFactory.newInstance(ctx);
                DocIdSetIterator approximation = DocIdSetIterator.all(ctx.reader().maxDoc());
                TwoPhaseIterator twoPhase = new TwoPhaseIterator(approximation) {
                    @Override
                    public boolean matches() throws IOException {
                        return false == script.resultsForDoc(approximation().docID()).isEmpty();
                    }

                    @Override
                    public float matchCost() {
                        // TODO we don't have a good way of estimating the complexity of the script so we just go with 9000
                        return 9000f;
                    }
                };
                return new ConstantScoreScorer(this, score(), scoreMode, twoPhase);
            }
        };
    }

    @Override
    public void visit(QueryVisitor visitor) {}

    @Override
    public final String toString(String field) {
        if (fieldName.contentEquals(field)) {
            return "*";
        }
        return fieldName + ":*";
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        StringScriptFieldExistsQuery other = (StringScriptFieldExistsQuery) obj;
        return fieldName.equals(other.fieldName);
    }
}
