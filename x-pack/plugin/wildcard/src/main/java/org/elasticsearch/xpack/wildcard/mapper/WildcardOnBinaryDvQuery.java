/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;

import java.io.IOException;
import java.util.Objects;

/**
 * Query that runs a wildcard pattern across all binary doc values. 
 * Expensive to run so normally used in conjunction with more selective query clauses.
 */
public class WildcardOnBinaryDvQuery extends Query {

    private final String field;
    private final String wildcardPattern;
    private Automaton automaton;

    public WildcardOnBinaryDvQuery(String field, String wildcardPattern, Automaton automaton) {
        this.field = field;
        this.wildcardPattern = wildcardPattern;
        this.automaton = automaton;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
                
        ByteRunAutomaton bytesMatcher = new ByteRunAutomaton(automaton);
        
        return new ConstantScoreWeight(this, boost) {

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                final BinaryDocValues values = DocValues.getBinary(context.reader(), field);               
                TwoPhaseIterator twoPhase = new TwoPhaseIterator(values) {
                    @Override
                    public boolean matches() throws IOException {
                        BytesRef value = values.binaryValue();
                        return  bytesMatcher.run(value.bytes, value.offset, value.length);
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
        WildcardOnBinaryDvQuery other = (WildcardOnBinaryDvQuery) obj;
        return Objects.equals(field, other.field)  && Objects.equals(wildcardPattern, other.wildcardPattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, wildcardPattern);
    }

}
