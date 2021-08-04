/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.wildcard.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;

import java.io.IOException;
import java.util.Objects;

/**
 * Query that runs an Automaton across all binary doc values.
 * Expensive to run so normally used in conjunction with more selective query clauses.
 */
public class BinaryDvConfirmedAutomatonQuery extends Query {

    private final String field;
    private final String matchPattern;
    private final ByteRunAutomaton bytesMatcher;
    private final Query approxQuery;

    public BinaryDvConfirmedAutomatonQuery(Query approximation, String field, String matchPattern, Automaton automaton) {
        this.approxQuery = approximation;
        this.field = field;
        this.matchPattern = matchPattern;
        bytesMatcher = new ByteRunAutomaton(automaton);
    }
    
    private BinaryDvConfirmedAutomatonQuery(Query approximation, String field, String matchPattern, ByteRunAutomaton bytesMatcher) {
        this.approxQuery = approximation;
        this.field = field;
        this.matchPattern = matchPattern;
        this.bytesMatcher = bytesMatcher;
    }    
    
    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query approxRewrite = approxQuery.rewrite(reader);
        if (approxQuery != approxRewrite ) {
            return new BinaryDvConfirmedAutomatonQuery(approxRewrite, field, matchPattern, bytesMatcher);
        }
        return this;
    }    

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                ByteArrayDataInput badi = new ByteArrayDataInput();
                final BinaryDocValues values = DocValues.getBinary(context.reader(), field);
                TwoPhaseIterator twoPhase = new TwoPhaseIterator(values) {
                    @Override
                    public boolean matches() throws IOException {
                        BytesRef arrayOfValues = values.binaryValue();
                        badi.reset(arrayOfValues.bytes);
                        badi.setPosition(arrayOfValues.offset);

                        int size = badi.readVInt();
                        for (int i=0; i< size; i++) {
                            int valLength = badi.readVInt();
                            if (bytesMatcher.run(arrayOfValues.bytes, badi.getPosition(), valLength)) {
                                return true;
                            }
                            badi.skipBytes(valLength);
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
        return field+":"+matchPattern;
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        BinaryDvConfirmedAutomatonQuery other = (BinaryDvConfirmedAutomatonQuery) obj;
        return Objects.equals(field, other.field)  && Objects.equals(matchPattern, other.matchPattern)
            && Objects.equals(bytesMatcher, other.bytesMatcher) 
            && Objects.equals(approxQuery, other.approxQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, matchPattern, bytesMatcher, approxQuery);
    }

}
