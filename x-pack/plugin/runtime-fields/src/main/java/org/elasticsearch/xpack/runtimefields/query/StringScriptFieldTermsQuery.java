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
import java.util.Set;

public class StringScriptFieldTermsQuery extends Query {
    private final StringScriptFieldScript.LeafFactory leafFactory;
    private final String fieldName;
    private final Set<String> terms;

    public StringScriptFieldTermsQuery(StringScriptFieldScript.LeafFactory leafFactory, String fieldName, Set<String> terms) {
        this.leafFactory = leafFactory;
        this.fieldName = fieldName;
        this.terms = terms;
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
                            if (terms.contains(result)) {
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
        if (visitor.acceptField(fieldName)) {
            for (String term : terms) {
                visitor.consumeTerms(this, new Term(fieldName, term));
            }
        }
    }

    @Override
    public final String toString(String field) {
        if (fieldName.contentEquals(field)) {
            return terms.toString();
        }
        return fieldName + ":" + terms;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, terms);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        StringScriptFieldTermsQuery other = (StringScriptFieldTermsQuery) obj;
        return fieldName.equals(other.fieldName) && terms.equals(other.terms);
    }
}
