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

/**
 *
 */
@Deprecated
public class BoostScoreFunction extends ScoreFunction {

    public static final String BOOST_WEIGHT_ERROR_MESSAGE = "'boost_factor' and 'weight' cannot be used together. Use 'weight'.";

    private final float boost;

    public BoostScoreFunction(float boost) {
        super(CombineFunction.MULT);
        this.boost = boost;
    }

    public float getBoost() {
        return boost;
    }

    @Override
    public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) {
        return new LeafScoreFunction() {

            @Override
            public double score(int docId, float subQueryScore) {
                return boost;
            }

            @Override
            public Explanation explainScore(int docId, Explanation subQueryScore) {
                return Explanation.match(boost, "static boost factor", Explanation.match(boost, "boostFactor"));
            }
        };
    }

    @Override
    public String toString() {
        return "boost[" + boost + "]";
    }

}
