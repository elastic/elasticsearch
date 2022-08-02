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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;

import java.io.IOException;
import java.util.Objects;

/**
 * Query that runs an Automaton across all binary doc values (but only for docs that also
 * match a provided approximation query which is key to getting good performance).
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
        if (approxQuery != approxRewrite) {
            return new BinaryDvConfirmedAutomatonQuery(approxRewrite, field, matchPattern, bytesMatcher);
        }
        return this;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        final Weight approxWeight = approxQuery.createWeight(searcher, scoreMode, boost);

        return new ConstantScoreWeight(this, boost) {

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                ByteArrayStreamInput bytes = new ByteArrayStreamInput();
                final BinaryDocValues values = DocValues.getBinary(context.reader(), field);
                Scorer approxScorer = approxWeight.scorer(context);
                if (approxScorer == null) {
                    // No matches to be had
                    return null;
                }
                DocIdSetIterator approxDisi = approxScorer.iterator();
                TwoPhaseIterator twoPhase = new TwoPhaseIterator(approxDisi) {
                    @Override
                    public boolean matches() throws IOException {
                        if (values.advanceExact(approxDisi.docID()) == false) {
                            // Can happen when approxQuery resolves to some form of MatchAllDocs expression
                            return false;
                        }
                        BytesRef arrayOfValues = values.binaryValue();
                        bytes.reset(arrayOfValues.bytes);
                        bytes.setPosition(arrayOfValues.offset);

                        int size = bytes.readVInt();
                        for (int i = 0; i < size; i++) {
                            int valLength = bytes.readVInt();
                            if (bytesMatcher.run(arrayOfValues.bytes, bytes.getPosition(), valLength)) {
                                return true;
                            }
                            bytes.skipBytes(valLength);
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
        return field + ":" + matchPattern;
    }

    @Override
    public boolean equals(Object obj) {
        if (sameClassAs(obj) == false) {
            return false;
        }
        BinaryDvConfirmedAutomatonQuery other = (BinaryDvConfirmedAutomatonQuery) obj;
        return Objects.equals(field, other.field)
            && Objects.equals(matchPattern, other.matchPattern)
            && Objects.equals(bytesMatcher, other.bytesMatcher)
            && Objects.equals(approxQuery, other.approxQuery);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), field, matchPattern, bytesMatcher, approxQuery);
    }

    Query getApproximationQuery() {
        return approxQuery;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }
}
