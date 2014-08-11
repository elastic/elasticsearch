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

/**
 *
 */
public class WeightFactorFunction extends ScoreFunction {


    public WeightFactorFunction(double weight) {
        super(CombineFunction.MULT);
        this.setWeight(weight);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
        // nothing to do here...
    }

    @Override
    public double score(int docId, float subQueryScore) {
        return 1.0;
    }

    @Override
    public Explanation explainScore(int docId, float score) {
        return new Explanation(score, "weight");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        WeightFactorFunction that = (WeightFactorFunction) o;

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
}
