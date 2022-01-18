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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * A function_score function that multiplies the score with the value of a
 * field from the document, optionally multiplying the field by a factor first,
 * and applying a modification (log, ln, sqrt, square, etc) afterwards.
 */
public class FieldValueFactorFunction extends ScoreFunction {
    private final String field;
    private final float boostFactor;
    private final Modifier modifier;
    /**
     * Value used if the document is missing the field.
     */
    private final Double missing;
    private final IndexNumericFieldData indexFieldData;

    public FieldValueFactorFunction(
        String field,
        float boostFactor,
        Modifier modifierType,
        Double missing,
        IndexNumericFieldData indexFieldData
    ) {
        super(CombineFunction.MULTIPLY);
        this.field = field;
        this.boostFactor = boostFactor;
        this.modifier = modifierType;
        this.indexFieldData = indexFieldData;
        this.missing = missing;
    }

    @Override
    public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) {
        final SortedNumericDoubleValues values;
        if (indexFieldData == null) {
            values = FieldData.emptySortedNumericDoubles();
        } else {
            values = this.indexFieldData.load(ctx).getDoubleValues();
        }

        return new LeafScoreFunction() {

            @Override
            public double score(int docId, float subQueryScore) throws IOException {
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
                double val = value * boostFactor;
                double result = modifier.apply(val);
                if (result < 0f) {
                    String message = "field value function must not produce negative scores, but got: "
                        + "["
                        + result
                        + "] for field value: ["
                        + value
                        + "]";
                    if (modifier == Modifier.LN) {
                        message += "; consider using ln1p or ln2p instead of ln to avoid negative scores";
                    } else if (modifier == Modifier.LOG) {
                        message += "; consider using log1p or log2p instead of log to avoid negative scores";
                    }
                    throw new IllegalArgumentException(message);
                }
                return result;
            }

            @Override
            public Explanation explainScore(int docId, Explanation subQueryScore) throws IOException {
                String modifierStr = modifier != null ? modifier.toString() : "";
                String defaultStr = missing != null ? "?:" + missing : "";
                double score = score(docId, subQueryScore.getValue().floatValue());
                return Explanation.match(
                    (float) score,
                    String.format(
                        Locale.ROOT,
                        "field value function: %s(doc['%s'].value%s * factor=%s)",
                        modifierStr,
                        field,
                        defaultStr,
                        boostFactor
                    )
                );
            }
        };
    }

    @Override
    public boolean needsScores() {
        return false;
    }

    @Override
    protected boolean doEquals(ScoreFunction other) {
        FieldValueFactorFunction fieldValueFactorFunction = (FieldValueFactorFunction) other;
        return this.boostFactor == fieldValueFactorFunction.boostFactor
            && Objects.equals(this.field, fieldValueFactorFunction.field)
            && Objects.equals(this.modifier, fieldValueFactorFunction.modifier);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(boostFactor, field, modifier);
    }

    /**
     * The Type class encapsulates the modification types that can be applied
     * to the score/value product.
     */
    public enum Modifier implements Writeable {
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
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        public static Modifier readFromStream(StreamInput in) throws IOException {
            return in.readEnum(Modifier.class);
        }

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }

        public static Modifier fromString(String modifier) {
            return valueOf(modifier.toUpperCase(Locale.ROOT));
        }
    }
}
