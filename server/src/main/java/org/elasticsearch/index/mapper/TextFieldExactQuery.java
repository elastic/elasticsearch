/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.LeafFieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import java.io.IOException;
import java.util.function.Function;

class TextFieldExactQuery extends Query {

    private final String field;
    private final BytesRef value;
    private final Query conjunction;
    private final Function<LeafReaderContext, LeafFieldData> fieldData;

    TextFieldExactQuery(MappedFieldType fieldType, Function<LeafReaderContext, LeafFieldData> fieldData, String input) throws IOException {
        this.field = fieldType.name();
        this.value = new BytesRef(input);
        this.fieldData = fieldData;
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        TokenStream ts = fieldType.getTextSearchInfo().searchAnalyzer().tokenStream(field, input);
        CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
        ts.reset();
        while (ts.incrementToken()) {
            bq.add(new TermQuery(new Term(field, termAtt.toString())), BooleanClause.Occur.MUST);
        }
        this.conjunction = bq.build();
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        Weight conjWeight = this.conjunction.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1);
        return new ConstantScoreWeight(this, 1) {
            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                Scorer conjScorer = conjWeight.scorer(context);
                if (conjScorer == null) {
                    return null;
                }
                DocIdSetIterator approx = conjScorer.iterator();
                LeafFieldData fd = fieldData.apply(context);
                SortedBinaryDocValues dv = fd.getBytesValues();
                TwoPhaseIterator twoPhase = new TwoPhaseIterator(approx) {
                    @Override
                    public boolean matches() throws IOException {
                        if (dv.advanceExact(approximation.docID()) == false) {
                            return false;
                        }
                        int values = dv.docValueCount();
                        for (int i = 0; i < values; i++) {
                            BytesRef v = dv.nextValue();
                            if (v.equals(value)) {
                                return true;
                            }
                        }
                        return false;
                    }

                    @Override
                    public float matchCost() {
                        return 9000;
                    }
                };
                return new ConstantScoreScorer(this, score(), scoreMode, twoPhase);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }
        };
    }

    @Override
    public String toString(String field) {
        return field + ":exact(" + value.utf8ToString() + ")";
    }

    @Override
    public void visit(QueryVisitor visitor) {
        conjunction.visit(visitor);
    }

    @Override
    public boolean equals(Object obj) {
        return false;
    }

    @Override
    public int hashCode() {
        return 0;
    }
}
