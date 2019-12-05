/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;

import java.io.IOException;
import java.util.Objects;

/**
 * Query that runs a wildcard pattern across all doc values. 
 * Expensive to run so normally used in conjunction with more selective query clauses.
 */
public class WildcardOnDvQuery extends Query {

    private final String field;
    private final String wildcardPattern;

    public WildcardOnDvQuery(String field, String wildcardPattern) {
        this.field = field;
        this.wildcardPattern = wildcardPattern;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        
        Automaton automaton = WildcardQuery.toAutomaton(new Term(field,wildcardPattern));
        ByteRunAutomaton bytesMatcher = new ByteRunAutomaton(automaton);
        
        return new ConstantScoreWeight(this, boost) {

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                SortedSetDocValues values = DocValues.getSortedSet(context.reader(), field);                
                TwoPhaseIterator twoPhase = new TwoPhaseIterator(values) {
                    @Override
                    public boolean matches() throws IOException {
                        long ord = values.nextOrd();
                        while (ord != SortedSetDocValues.NO_MORE_ORDS) {
                            BytesRef value = values.lookupOrd(ord);
                            if (bytesMatcher.run(value.bytes, value.offset, value.length)) {
                                return true;
                            }
                            ord = values.nextOrd();
                        }
                        return false;
                    }

                    @Override
                    public float matchCost() {
                        // TODO: how can we compute this?
                        return 1000f;
                    }
                };
                return new ConstantScoreScorer(this, score(), scoreMode, twoPhase);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }
        };
    }
    @Override
    public String toString(String field) {
        return field+":"+wildcardPattern;
    }

    @Override
    public boolean equals(Object obj) {
        WildcardOnDvQuery other = (WildcardOnDvQuery) obj;
        return Objects.equals(field, other.field)  && Objects.equals(wildcardPattern, other.wildcardPattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, wildcardPattern);
    }

}
