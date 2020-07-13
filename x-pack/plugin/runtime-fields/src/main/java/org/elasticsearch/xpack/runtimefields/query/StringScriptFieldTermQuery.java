/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
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

public class StringScriptFieldTermQuery extends Query {
    private final StringScriptFieldScript.LeafFactory leafFactory;
    private final String fieldName;
    private final String term;

    public StringScriptFieldTermQuery(StringScriptFieldScript.LeafFactory leafFactory, String fieldName, String term) {
        this.leafFactory = leafFactory;
        this.fieldName = fieldName;
        this.term = term;
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
                        for (String result : script.resultsForDoc(approximation().docID())) {
                            if (term.equals(result)) {
                                return true;
                            }
                        }
                        return false;
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
    public void visit(QueryVisitor visitor) {
        visitor.consumeTerms(this, new Term(fieldName, term));
    }

    @Override
    public final String toString(String field) {
        if (fieldName.contentEquals(field)) {
            return term;
        }
        return fieldName + ":" + term;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, term);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        StringScriptFieldTermQuery other = (StringScriptFieldTermQuery) obj;
        return fieldName.equals(other.fieldName) && term.equals(other.term);
    }
}
