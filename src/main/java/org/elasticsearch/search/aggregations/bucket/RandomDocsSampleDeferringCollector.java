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

package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;

/**
 * A {@link DeferringBucketCollector} that first identifies a random sample of
 * documents in search results and then passes only these on to nested
 * collectors. This implementation is only for use with a single bucket
 * aggregation.
 */
public class RandomDocsSampleDeferringCollector extends BestDocsDeferringCollector {

    public RandomDocsSampleDeferringCollector(BucketCollector deferred, AggregationContext context, int shardSize) {
        super(deferred, context, shardSize);
    }

    @Override
    protected TopDocsCollector<RandomSampleScoreDoc> createTopDocsCollector(int size) {
        return new RandomDocSampleCollector(size);
    }

    static class RandomDocSampleCollector extends TopDocsCollector<RandomSampleScoreDoc> {
        RandomSampleScoreDoc pqTop;
        int docBase = 0;
        Scorer scorer;


        public RandomDocSampleCollector(int numHits) {
            super(new SampledHitQueue(numHits, true));
            // SampledHitQueue implements getSentinelObject to return a
            // ScoreDoc, so we
            // know that at this point top() is already initialized.
            pqTop = pq.top();
        }

        @Override
        protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
            if (results == null) {
                return EMPTY_TOPDOCS;
            }

            // We need to compute maxScore in order to set it in TopDocs. If
            // start == 0, it means the largest element is already in results,
            // use its
            // score as maxScore. Otherwise pop everything else, until the
            // largest element is extracted and use its score as maxScore.
            float maxScore = Float.NaN;
            if (start == 0) {
                maxScore = ((RandomSampleScoreDoc) results[0]).randomizer;
            } else {
                for (int i = pq.size(); i > 1; i--) {
                    pq.pop();
                }
                maxScore = ((RandomSampleScoreDoc) pq.pop()).randomizer;
            }

            return new TopDocs(totalHits, results, maxScore);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) {
            docBase = context.docBase;
        }

        @Override
        public void setScorer(Scorer scorer) throws IOException {
            this.scorer = scorer;
        }

        @Override
        public void collect(int doc) throws IOException {
            float random = (float) Math.random();
            float realScore = scorer.score();

            // This collector cannot handle NaN
            assert !Float.isNaN(random);

            totalHits++;
            if (random < pqTop.randomizer) {
                // Doesn't compete w/ bottom entry in queue
                return;
            }
            doc += docBase;
            if (random == pqTop.randomizer && doc > pqTop.doc) {
                // Break tie in score by doc ID:
                return;
            }
            pqTop.doc = doc;
            pqTop.score = realScore;
            pqTop.randomizer = random;
            pqTop = pq.updateTop();
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
            return false;
        }

        static class SampledHitQueue extends PriorityQueue<RandomSampleScoreDoc> {

            SampledHitQueue(int size, boolean prePopulate) {
                super(size, prePopulate);
            }

            @Override
            protected RandomSampleScoreDoc getSentinelObject() {
                // Always set the doc Id to MAX_VALUE so that it won't be
                // favored by lessThan.
                return new RandomSampleScoreDoc(Integer.MAX_VALUE, Float.NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY);
            }

            @Override
            protected final boolean lessThan(RandomSampleScoreDoc hitA, RandomSampleScoreDoc hitB) {
                if (hitA.randomizer == hitB.randomizer)
                    return hitA.doc > hitB.doc;
                else
                    return hitA.randomizer < hitB.randomizer;
            }
        }
    }

    static class RandomSampleScoreDoc extends ScoreDoc {
        float randomizer;

        public RandomSampleScoreDoc(int doc, float score) {
            super(doc, score);
            randomizer = (float) Math.random();
        }

        // Constructor used for sentinel object
        protected RandomSampleScoreDoc(int doc, float score, float randomizer) {
            super(doc, score);
            this.randomizer = randomizer;
        }
    }

}
