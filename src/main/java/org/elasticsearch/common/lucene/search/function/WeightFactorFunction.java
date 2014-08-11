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
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.ElasticsearchIllegalArgumentException;

/**
 *
 */
public class WeightFactorFunction extends ScoreFunction {


    private final ScoreFunction scoreFunction;
    private double weight = 1.0;

    public WeightFactorFunction(double weight, ScoreFunction scoreFunction) {
        super(CombineFunction.MULT);
        if (scoreFunction instanceof BoostScoreFunction) {
            throw new ElasticsearchIllegalArgumentException(BoostScoreFunction.BOOST_WEIGHT_ERROR_MESSAGE_PARSER);
        }
        this.scoreFunction = scoreFunction;
        this.weight = weight;
    }

    public WeightFactorFunction(double weight) {
        super(CombineFunction.MULT);
        this.scoreFunction = null;
        this.weight = weight;
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        if (scoreFunction != null ) {
            scoreFunction.setNextReader(context);
        }
    }

    @Override
    public double score(int docId, float subQueryScore) {
        if (scoreFunction != null ) {
            return scoreFunction.score(docId, subQueryScore) * getWeight();
        } else {
            return getWeight();
        }
    }

    @Override
    public Explanation explainScore(int docId, float score) {
        Explanation functionScoreExplanation;
        if (scoreFunction != null) {
            Explanation functionExplanation = scoreFunction.explainScore(docId, score);
             functionScoreExplanation = new ComplexExplanation(true, functionExplanation.getValue() * (float) getWeight(), "product of:");
            functionScoreExplanation.addDetail(functionExplanation);
            functionScoreExplanation.addDetail(explainWeight());
        } else {
            functionScoreExplanation = explainWeight();
        }
        return functionScoreExplanation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        WeightFactorFunction that = (WeightFactorFunction) o;
        if (! scoreFunction.equals(that.scoreFunction)) {
            return false;
        }

        if (that.getWeight() != getWeight())
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (getWeight() != +0.0f ? Float.floatToIntBits((float) getWeight()) : 0);
    }

    @Override
    public String toString() {
        return "weight[" + getWeight() + "]";
    }

    public Explanation explainWeight() {
        return new Explanation((float) getWeight(), "weight");
    }


    public double getWeight() {
        return weight;
    }
}
