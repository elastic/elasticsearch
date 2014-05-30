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
package org.elasticsearch.index.search.child;

import com.carrotsearch.hppc.FloatArrayList;
import org.apache.lucene.search.Scorer;

import java.io.IOException;

class MockScorer extends Scorer {

    final ScoreType scoreType;
    FloatArrayList scores;

    MockScorer(ScoreType scoreType) {
        super(null);
        this.scoreType = scoreType;
    }

    @Override
    public float score() throws IOException {
        if (scoreType == ScoreType.NONE) {
            return 1.0f;
        }
        float aggregateScore = 0;
        for (int i = 0; i < scores.elementsCount; i++) {
            float score = scores.buffer[i];
            switch (scoreType) {
                case MAX:
                    if (aggregateScore < score) {
                        aggregateScore = score;
                    }
                    break;
                case SUM:
                case AVG:
                    aggregateScore += score;
                    break;
            }
        }

        if (scoreType == ScoreType.AVG) {
            aggregateScore /= scores.elementsCount;
        }

        return aggregateScore;
    }

    @Override
    public int freq() throws IOException {
        return 0;
    }

    @Override
    public int docID() {
        return 0;
    }

    @Override
    public int nextDoc() throws IOException {
        return 0;
    }

    @Override
    public int advance(int target) throws IOException {
        return 0;
    }

    @Override
    public long cost() {
        return 0;
    }
}
