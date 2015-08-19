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

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.elasticsearch.search.profile.InternalProfileBreakdown;
import org.elasticsearch.search.profile.InternalProfiler;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;


/**
 * A wrapper abstract class, whose only purpose is to organize
 * useful components like ProfileWeight and ProfileScorer
 */
public abstract class ProfileQuery {

    /**
     * ProfileWeight wraps the query's weight and performs timing on:
     *  - scorer()
     *  - bulkScorer()
     *  - normalize()
     *
     * The rest of the methods are delegated to the wrapped weight directly
     * without timing.
     */
    public static class ProfileWeight extends Weight {

        final Weight subQueryWeight;
        private final InternalProfiler profiler;

        public ProfileWeight(Query query, Weight subQueryWeight, InternalProfiler profiler) throws IOException {
            super(query);
            this.subQueryWeight = subQueryWeight;
            this.profiler = profiler;
        }

        @Override
        public Scorer scorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
            profiler.startTime(getQuery(), InternalProfileBreakdown.TimingType.BUILD_SCORER);
            Scorer subQueryScorer = subQueryWeight.scorer(context, acceptDocs);
            profiler.stopAndRecordTime(getQuery(), InternalProfileBreakdown.TimingType.BUILD_SCORER);
            if (subQueryScorer == null) {
                return null;
            }

            return new ProfileScorer(this, subQueryScorer, profiler, getQuery());
        }

        @Override
        public BulkScorer bulkScorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
            profiler.startTime(getQuery(), InternalProfileBreakdown.TimingType.BUILD_SCORER);
            BulkScorer bScorer = subQueryWeight.bulkScorer(context, acceptDocs);
            profiler.stopAndRecordTime(getQuery(), InternalProfileBreakdown.TimingType.BUILD_SCORER);

            if (bScorer == null) {
                return null;
            }

            return new ProfileBulkScorer(bScorer, profiler, getQuery());
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            return subQueryWeight.explain(context, doc);
        }

        @Override
        public float getValueForNormalization() throws IOException {
            return subQueryWeight.getValueForNormalization();
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
            profiler.startTime(getQuery(), InternalProfileBreakdown.TimingType.NORMALIZE);
            subQueryWeight.normalize(norm, topLevelBoost);
            profiler.stopAndRecordTime(getQuery(), InternalProfileBreakdown.TimingType.NORMALIZE);
        }

        @Override
        public void extractTerms(Set<Term> set) {
            subQueryWeight.extractTerms(set);
        }
    }


    /**
     * ProfileScorer wraps the query's scorer and performs timing on:
     *  - score()
     *
     * The rest of the methods are delegated to the wrapped scorer directly
     * without any timing.  Notably, docID(), advance() and nextDoc() are
     * not timed since those are called recursively and will inflate timings
     */
    public static class ProfileScorer extends Scorer {

        private final Scorer scorer;
        private ProfileWeight profileWeight;
        private final InternalProfiler profiler;
        private final Query query;

        private ProfileScorer(ProfileWeight w, Scorer scorer, InternalProfiler profiler, Query query) throws IOException {
            super(w);
            this.scorer = scorer;
            this.profileWeight = w;
            this.profiler = profiler;
            this.query = query;
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
        public float score() throws IOException {
            profiler.startTime(query, InternalProfileBreakdown.TimingType.SCORE);
            float score = scorer.score();
            profiler.stopAndRecordTime(query, InternalProfileBreakdown.TimingType.SCORE);

            return score;
        }

        @Override
        public int freq() throws IOException {
            return scorer.freq();
        }

        @Override
        public long cost() {
            return scorer.cost();
        }

        @Override
        public Weight getWeight() {
            return profileWeight;
        }

        @Override
        public Collection<ChildScorer> getChildren() {
            return scorer.getChildren();
        }

        @Override
        public TwoPhaseIterator asTwoPhaseIterator() {
            return scorer.asTwoPhaseIterator();
        }
    }

    /**
     * ProfileBulkScorer wraps the query's bulk scorer and performs timing on:
     *  - score()
     *  - cost()
     */
    static class ProfileBulkScorer extends BulkScorer {

        private final InternalProfiler profiler;
        private final Query query;
        private final BulkScorer bulkScorer;

        public ProfileBulkScorer(BulkScorer bulkScorer, InternalProfiler profiler, Query query) {
            super();
            this.bulkScorer = bulkScorer;
            this.profiler = profiler;
            this.query = query;
        }

        @Override
        public void score(LeafCollector collector) throws IOException {
            profiler.startTime(query, InternalProfileBreakdown.TimingType.SCORE);
            bulkScorer.score(collector);
            profiler.stopAndRecordTime(query, InternalProfileBreakdown.TimingType.SCORE);
        }

        @Override
        public int score(LeafCollector collector, int min, int max) throws IOException {
            profiler.startTime(query, InternalProfileBreakdown.TimingType.SCORE);
            int score = bulkScorer.score(collector, min, max);
            profiler.stopAndRecordTime(query, InternalProfileBreakdown.TimingType.SCORE);
            return score;
        }

        @Override
        public long cost() {
            profiler.startTime(query, InternalProfileBreakdown.TimingType.COST);
            long cost = bulkScorer.cost();
            profiler.stopAndRecordTime(query, InternalProfileBreakdown.TimingType.COST);
            return cost;
        }
    }
}