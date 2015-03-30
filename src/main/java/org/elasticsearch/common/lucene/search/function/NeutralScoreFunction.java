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
import org.elasticsearch.ElasticsearchIllegalStateException;

import java.io.IOException;

/**
 * This is an artifical function that just returns depending on boost_mode
 * 0 for sum or avg
 * 1 for multiply
 * FLOAT.negative inf for max
 * FLOAT.positive inf for min
 * 1 for replace
 */
public class NeutralScoreFunction extends ScoreFunction {


    private final CombineFunction combineFunction;

    public NeutralScoreFunction(CombineFunction combineFunction) {
        super(combineFunction);
        this.combineFunction = combineFunction;
    }

    @Override
    public void setNextReader(LeafReaderContext context) throws IOException {
    }

    @Override
    public double score(int docId, float subQueryScore) {
        if (combineFunction.equals(CombineFunction.SUM)) {
            return 0;
        } else if (combineFunction.equals(CombineFunction.MULT)) {
            return 1;
        } else if (combineFunction.equals(CombineFunction.MIN)) {
            return Float.POSITIVE_INFINITY;
        } else if (combineFunction.equals(CombineFunction.MAX)) {
            return Float.NEGATIVE_INFINITY;
        } else if (combineFunction.equals(CombineFunction.AVG)) {
            return subQueryScore;
        } else if (combineFunction.equals(CombineFunction.REPLACE)) {
            return 1;
        } else {
            throw new ElasticsearchIllegalStateException("Score combine function not known: " + combineFunction.getName());
        }
    }

    @Override
    public Explanation explainScore(int docId, Explanation subQueryScore) throws IOException {
        Explanation functionScoreExplanation = new Explanation((float) score(docId, subQueryScore.getValue()), "neutral element because no query was given");
        return functionScoreExplanation;
    }
}
