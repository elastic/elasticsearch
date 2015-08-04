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

import com.google.common.base.Stopwatch;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.elasticsearch.search.profile.TimingWrapper;
import org.elasticsearch.search.query.InternalProfiler;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;


/**
 * This class times the execution of the subquery that it wraps.  Timing includes:
 *  - ProfileQuery.createWeight
 *
 *  - ProfileWeight.getValueForNormalization
 *  - ProfileWeight.normalize
 *
 *  - ProfileScorer.advance
 *  - ProfileScorer.nextDoc
 *  - ProfileScorer.score
 *
 *  A ProfileQuery maintains it's own timing independent of the rest of the query.
 *  It must be later aggregated together using Profile.collapse
 */
public class ProfileQuery extends Query {

    Query subQuery;

    public ProfileQuery(Query subQuery) {
        setSubQuery(subQuery);
    }

    public void setSubQuery(Query subQuery) {
        this.subQuery = subQuery;
    }

    /** Create a shallow copy of us -- used in rewriting if necessary
     * @return a copy of us (but reuse, don't copy, our subqueries) */
    @Override
    public Query clone() {
        ProfileQuery p = (ProfileQuery)super.clone();
        p.subQuery = this.subQuery;
        return p;
    }


    public String toString(String field) {
        StringBuilder sb = new StringBuilder();

        // Currently only outputting the subquery's string.  This makes the ProfileQuery "invisible"
        // in explains/analyze, but makes the output much nicer for profiling
        sb.append(subQuery.toString(field));
        return sb.toString();
    }

    @Override
    public void setBoost(float b) {
        subQuery.setBoost(b);
    }

    @Override
    public float getBoost() {
        return subQuery.getBoost();
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        return subQuery.rewrite(reader);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        return subQuery.createWeight(searcher, needsScores);
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ProfileQuery other = (ProfileQuery) o;
        return this.subQuery.equals(other.subQuery)
                && Float.floatToIntBits(this.getBoost()) == Float.floatToIntBits(other.getBoost());
    }

    public int hashCode() {
        final int prime = 31;
        int result = prime * 19;
        result = prime * result + this.subQuery.hashCode();
        result = prime * result + Float.floatToIntBits(getBoost());
        return result;
    }

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
            profiler.startTime(getQuery(), TimingWrapper.TimingType.BUILD_SCORER);
            Scorer subQueryScorer = subQueryWeight.scorer(context, acceptDocs);
            profiler.stopAndRecordTime(getQuery(), TimingWrapper.TimingType.BUILD_SCORER);
            if (subQueryScorer == null) {
                return null;
            }

            return new ProfileScorer(this, subQueryScorer, profiler, getQuery());
        }

        @Override
        public BulkScorer bulkScorer(LeafReaderContext context, Bits acceptDocs) throws IOException {
            profiler.startTime(getQuery(), TimingWrapper.TimingType.BUILD_SCORER);
            BulkScorer bScorer = subQueryWeight.bulkScorer(context, acceptDocs);
            profiler.stopAndRecordTime(getQuery(), TimingWrapper.TimingType.BUILD_SCORER);

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
            profiler.startTime(getQuery(), TimingWrapper.TimingType.NORMALIZE);
            subQueryWeight.normalize(norm, topLevelBoost);
            profiler.stopAndRecordTime(getQuery(), TimingWrapper.TimingType.NORMALIZE);
        }

        @Override
        public void extractTerms(Set<Term> set) {
            subQueryWeight.extractTerms(set);
        }
    }




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
            profiler.startTime(query, TimingWrapper.TimingType.SCORE);
            float score = scorer.score();
            profiler.stopAndRecordTime(query, TimingWrapper.TimingType.SCORE);

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
    }

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
            profiler.startTime(query, TimingWrapper.TimingType.SCORE);
            bulkScorer.score(collector);
            profiler.stopAndRecordTime(query, TimingWrapper.TimingType.SCORE);
        }

        @Override
        public int score(LeafCollector collector, int min, int max) throws IOException {
            profiler.startTime(query, TimingWrapper.TimingType.SCORE);
            int score = bulkScorer.score(collector, min, max);
            profiler.stopAndRecordTime(query, TimingWrapper.TimingType.SCORE);
            return score;
        }

        @Override
        public long cost() {
            profiler.startTime(query, TimingWrapper.TimingType.COST);
            long cost = bulkScorer.cost();
            profiler.stopAndRecordTime(query, TimingWrapper.TimingType.COST);
            return cost;
        }
    }
}