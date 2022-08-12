/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.search.function;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class WeightFieldFactorFunction extends ScoreFunction {
    private final String field;
    private static final ScoreFunction SCORE_ONE = new ScoreOne();
    private final ScoreFunction scoreFunction;
    /**
     * Value used if the document is missing the field.
     */
    private final Double missing;
    private final IndexNumericFieldData indexFieldData;

    public WeightFieldFactorFunction(String field, Double missing, IndexNumericFieldData indexFieldData, ScoreFunction scoreFunction) {
        super(CombineFunction.MULTIPLY);
        this.field = field;
        this.missing = missing;
        this.indexFieldData = indexFieldData;
        if (scoreFunction == null) {
            this.scoreFunction = SCORE_ONE;
        } else {
            this.scoreFunction = scoreFunction;
        }
    }

    @Override
    public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) throws IOException {
        final SortedNumericDoubleValues values;
        if (indexFieldData == null) {
            values = FieldData.emptySortedNumericDoubles();
        } else {
            values = this.indexFieldData.load(ctx).getDoubleValues();
        }

        final LeafScoreFunction leafFunction = scoreFunction.getLeafScoreFunction(ctx);
        return new LeafScoreFunction() {
            private double value(int docId) throws IOException {
                double value;
                if (values.advanceExact(docId)) {
                    value = values.nextValue();
                } else {
                    if (missing != null) {
                        value = missing;
                    } else {
                        throw new ElasticsearchException("Missing value for field [" + field + "]");
                    }
                }

                return value;
            }

            @Override
            public double score(int docId, float subQueryScore) throws IOException {
                return leafFunction.score(docId, subQueryScore) * value(docId);
            }

            @Override
            public Explanation explainScore(int docId, Explanation subQueryScore) throws IOException {
                double value = value(docId);
                double score = leafFunction.score(docId, subQueryScore.getValue().floatValue()) * value(docId);

                Explanation functionExplanation = leafFunction.explainScore(docId, subQueryScore);
                return Explanation.match(
                    (float) score,
                    "product of:",
                    functionExplanation,
                    explainWeight((float) value)
                );
            }
        };
    }

    @Override
    public boolean needsScores() {
        return scoreFunction.needsScores();
    }

    public Explanation explainWeight(float weight) {
        String defaultStr = missing != null ? "?:" + missing : "";

        return Explanation.match(
            weight,
            String.format(
                Locale.ROOT,
                "weight field value function: doc['%s'].value%s",
                field,
                defaultStr
            )
        );
    }

    public String getField() {
        return field;
    }

    @Override
    protected boolean doEquals(ScoreFunction other) {
        WeightFieldFactorFunction weightFactorFunction = (WeightFieldFactorFunction) other;
        return this.field == weightFactorFunction.field && Objects.equals(this.scoreFunction, weightFactorFunction.scoreFunction);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, scoreFunction);
    }
}
