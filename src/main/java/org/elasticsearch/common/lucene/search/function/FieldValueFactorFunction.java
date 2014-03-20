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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.internal.SearchContext;

import java.util.List;
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
    private final boolean lenient;
    private AtomicReaderContext currentContext;

    public FieldValueFactorFunction(String field, float boostFactor, Modifier modifierType, boolean lenient) {
        super(CombineFunction.MULT);
        this.field = field;
        this.boostFactor = boostFactor;
        this.modifier = modifierType;
        this.lenient = lenient;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        this.currentContext = context;
    }

    @Override
    public double score(int docId, float subQueryScore) {
        SearchContext searchContext = SearchContext.current();
        FieldMapper mapper = searchContext.mapperService().smartNameFieldMapper(field);
        if (mapper != null) {
            AtomicFieldData data = searchContext.fieldData().getForField(mapper).load(currentContext);
            ScriptDocValues values = data.getScriptValues();
            values.setNextDocId(docId);
            List<?> actualValues = values.getValues();
            if (actualValues.size() > 0) {
                Object o = actualValues.get(0);
                double val;
                if (o instanceof Long) {
                    val = (long)o;
                } else if (o instanceof Double) {
                    val = (double)o;
                } else {
                    throw new ElasticsearchException("Invalid data type for field [" + field + "]");
                }
                return subQueryScore * Modifier.apply(modifier, val * boostFactor, lenient);
            } else {
                return subQueryScore;
            }
        }
        throw new ElasticsearchException("Unable to find a fieldmapper for field [" + field + "]");
    }

    @Override
    public Explanation explainScore(int docId, Explanation subQueryExpl) {
        Explanation exp = new Explanation();
        String modifierStr = modifier != null ? modifier.toString() : "";
        double score = score(docId, subQueryExpl.getValue());
        exp.setValue(CombineFunction.toFloat(score));
        exp.setDescription("field value function, score=" + score +
                " product of " + "_score=" + subQueryExpl.getValue() + " * " +
                modifierStr + "(" + "val * factor=" + boostFactor + ")");
        exp.addDetail(subQueryExpl);
        return exp;
    }

    /**
     * The Type class encapsulates the modification types that can be applied
     * to the score/value product.
     */
    public enum Modifier {
        NONE,
        LOG,
        LN,
        SQUARE,
        SQRT,
        RECIPROCAL;

        public static double apply(Modifier t, double n, boolean lenient) {
            if (t == null) {
                return n;
            }
            double result = n;
            switch (t) {
                case NONE:
                    break;
                case LOG:
                    result = Math.log10(n);
                    break;
                case LN:
                    result = Math.log(n);
                    break;
                case SQUARE:
                    result = Math.pow(n, 2);
                    break;
                case SQRT:
                    result = Math.sqrt(n);
                    break;
                case RECIPROCAL:
                    result = 1.0 / n;
                    break;
                default: throw new ElasticsearchIllegalArgumentException("Unknown modifier type: [" + t + "]");
            }
            if (Double.isNaN(result) || Double.isInfinite(result)) {
                if (lenient) {
                    return 0;
                } else {
                    throw new ElasticsearchException("Result of field modification [" + t.toString() +
                            "(" + n + ")] must be a number");
                }
            }
            return result;
        }

        @Override
        public String toString() {
            if (this == NONE) {
                return "";
            }
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }
}
