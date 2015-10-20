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

import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;

abstract class CustomBoostFactorScorer extends Scorer {

    final Scorer scorer;
    final float maxBoost;
    final CombineFunction scoreCombiner;

    CustomBoostFactorScorer(Weight w, Scorer scorer, float maxBoost, CombineFunction scoreCombiner)
            throws IOException {
        super(w);
        this.scorer = scorer;
        this.maxBoost = maxBoost;
        this.scoreCombiner = scoreCombiner;
    }

    @Override
    public int docID() {
        return scorer.docID();
    }

    @Override
    public int advance(int target) throws IOException {
        return scorer.advance(target);
    }

    @Override
    public int nextDoc() throws IOException {
        return scorer.nextDoc();
    }

    @Override
    public abstract float score() throws IOException;

    @Override
    public int freq() throws IOException {
        return scorer.freq();
    }

    @Override
    public long cost() {
        return scorer.cost();
    }

}
