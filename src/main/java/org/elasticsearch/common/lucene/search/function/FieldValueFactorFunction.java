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

package org.elasticsearch.common.lucene.search.function;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

import java.util.Locale;

/**
 * A function_score function that multiplies the score with the value of a
 * field from the document, optionally multiplying the field by a factor first,
 * and applying a modification (log, ln, sqrt, square, etc) afterwards.
 */
public class FieldValueFactorFunction extends ScoreFunction {
    private final String field;
    private final float boostFactor;
    private final Modifier modifier;
    private final IndexNumericFieldData indexFieldData;

    public FieldValueFactorFunction(String field, float boostFactor, Modifier modifierType, IndexNumericFieldData indexFieldData) {
        super(CombineFunction.MULT);
        this.field = field;
        this.boostFactor = boostFactor;
        this.modifier = modifierType;
        this.indexFieldData = indexFieldData;
    }

    @Override
    public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) {
        final SortedNumericDoubleValues values = this.indexFieldData.load(ctx).getDoubleValues();
        return new LeafScoreFunction() {

            @Override
            public double score(int docId, float subQueryScore) {
                values.setDocument(docId);
                final int numValues = values.count();
                if (numValues > 0) {
                    double val = values.valueAt(0) * boostFactor;
                    double result = modifier.apply(val);
                    if (Double.isNaN(result) || Double.isInfinite(result)) {
                        throw new ElasticsearchException("Result of field modification [" + modifier.toString() +
                                "(" + val + ")] must be a number");
                    }
                    return result;
                } else {
                    throw new ElasticsearchException("Missing value for field [" + field + "]");
                }
            }

            @Override
            public Explanation explainScore(int docId, Explanation subQueryScore) {
                String modifierStr = modifier != null ? modifier.toString() : "";
                double score = score(docId, subQueryScore.getValue());
                return Explanation.match(
                        CombineFunction.toFloat(score),
                        "field value function: " + modifierStr + "(" + "doc['" + field + "'].value * factor=" + boostFactor + ")");
            }
        };
    }

    /**
     * The Type class encapsulates the modification types that can be applied
     * to the score/value product.
     */
    public enum Modifier {
        NONE {
            @Override
            public double apply(double n) {
                return n;
            }
        },
        LOG {
            @Override
            public double apply(double n) {
                return Math.log10(n);
            }
        },
        LOG1P {
            @Override
            public double apply(double n) {
                return Math.log10(n + 1);
            }
        },
        LOG2P {
            @Override
            public double apply(double n) {
                return Math.log10(n + 2);
            }
        },
        LN {
            @Override
            public double apply(double n) {
                return Math.log(n);
            }
        },
        LN1P {
            @Override
            public double apply(double n) {
                return Math.log1p(n);
            }
        },
        LN2P {
            @Override
            public double apply(double n) {
                return Math.log1p(n + 1);
            }
        },
        SQUARE {
            @Override
            public double apply(double n) {
                return Math.pow(n, 2);
            }
        },
        SQRT {
            @Override
            public double apply(double n) {
                return Math.sqrt(n);
            }
        },
        RECIPROCAL {
            @Override
            public double apply(double n) {
                return 1.0 / n;
            }
        };

        public abstract double apply(double n);

        @Override
        public String toString() {
            if (this == NONE) {
                return "";
            }
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }
}
