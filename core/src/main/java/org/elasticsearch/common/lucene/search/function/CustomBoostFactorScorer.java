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

    Float minScore;
    NextDoc nextDoc;

    CustomBoostFactorScorer(Weight w, Scorer scorer, float maxBoost, CombineFunction scoreCombiner, Float minScore)
            throws IOException {
        super(w);
        if (minScore == null) {
            nextDoc = new AnyNextDoc();
        } else {
            nextDoc = new MinScoreNextDoc();
        }
        this.scorer = scorer;
        this.maxBoost = maxBoost;
        this.scoreCombiner = scoreCombiner;
        this.minScore = minScore;
    }

    @Override
    public int docID() {
        return scorer.docID();
    }

    @Override
    public int advance(int target) throws IOException {
        return nextDoc.advance(target);
    }

    @Override
    public int nextDoc() throws IOException {
        return nextDoc.nextDoc();
    }

    public abstract float innerScore() throws IOException;

    @Override
    public float score() throws IOException {
        return nextDoc.score();
    }

    @Override
    public int freq() throws IOException {
        return scorer.freq();
    }

    @Override
    public long cost() {
        return scorer.cost();
    }

    public interface NextDoc {
        public int advance(int target) throws IOException;

        public int nextDoc() throws IOException;

        public float score() throws IOException;
    }

    public class MinScoreNextDoc implements NextDoc {
        float currentScore = Float.MAX_VALUE * -1.0f;

        @Override
        public int nextDoc() throws IOException {
            int doc;
            do {
                doc = scorer.nextDoc();
                if (doc == NO_MORE_DOCS) {
                    return doc;
                }
                currentScore = innerScore();
            } while (currentScore < minScore);
            return doc;
        }

        @Override
        public float score() throws IOException {
            return currentScore;
        }

        @Override
        public int advance(int target) throws IOException {
            int doc = scorer.advance(target);
            if (doc == NO_MORE_DOCS) {
                return doc;
            }
            currentScore = innerScore();
            if (currentScore < minScore) {
                return scorer.nextDoc();
            }
            return doc;
        }
    }

    public class AnyNextDoc implements NextDoc {

        @Override
        public int nextDoc() throws IOException {
            return scorer.nextDoc();
        }

        @Override
        public float score() throws IOException {
            return innerScore();
        }

        @Override
        public int advance(int target) throws IOException {
            return scorer.advance(target);
        }
    }
}
