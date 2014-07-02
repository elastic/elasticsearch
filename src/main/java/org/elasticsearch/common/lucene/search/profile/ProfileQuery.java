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

package org.elasticsearch.common.lucene.search.profile;

import com.google.common.base.Stopwatch;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;

import java.io.IOException;
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
public class ProfileQuery extends Query implements ProfileComponent {

    Query subQuery;
    private long time = 0;

    private String className;
    private String details;

    public ProfileQuery(Query subQuery) {
        this.subQuery = subQuery;
        this.setClassName(subQuery.getClass().getSimpleName());
        this.setDetails(subQuery.toString());
    }

    public Query subQuery() {
        return subQuery;
    }

    public long time() {
        return this.time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public void addTime(long time) {
        this.time += time;
    }

    public String className() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String details() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }


    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Query rewrittenQuery = subQuery.rewrite(reader);
        stopwatch.stop();
        addTime(stopwatch.elapsed(TimeUnit.MICROSECONDS));

        if (rewrittenQuery == subQuery) {
            return this;
        }

        // The rewriting process can potentially add many new nested components
        // Perform a walk of the rewritten query to wrap all the new parts
        ProfileQueryVisitor walker = new ProfileQueryVisitor();
        rewrittenQuery = (ProfileQuery) walker.apply(rewrittenQuery);

        this.subQuery = rewrittenQuery;
        return this;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
        subQuery.extractTerms(terms);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Weight subQueryWeight = subQuery.createWeight(searcher);
        stopwatch.stop();
        addTime(stopwatch.elapsed(TimeUnit.MICROSECONDS));

        return new ProfileWeight(subQueryWeight, this);
    }

    class ProfileWeight extends Weight {

        final Weight subQueryWeight;
        private ProfileQuery profileQuery;

        public ProfileWeight(Weight subQueryWeight, ProfileQuery profileQuery) throws IOException {
            this.subQueryWeight = subQueryWeight;
            this.profileQuery = profileQuery;
        }

        public Query getQuery() {
            return ProfileQuery.this;
        }

        public void addTime(long time) {
            this.profileQuery.addTime(time);
        }

        @Override
        public float getValueForNormalization() throws IOException {
            Stopwatch stopwatch = Stopwatch.createStarted();
            float sum = subQueryWeight.getValueForNormalization();
            stopwatch.stop();
            addTime(stopwatch.elapsed(TimeUnit.MICROSECONDS));

            return sum;
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            subQueryWeight.normalize(norm, topLevelBoost * getBoost());
            stopwatch.stop();
            addTime(stopwatch.elapsed(TimeUnit.MICROSECONDS));
        }

        @Override
        public Scorer scorer(AtomicReaderContext context, Bits acceptDocs) throws IOException {
            Scorer subQueryScorer = subQueryWeight.scorer(context, acceptDocs);
            if (subQueryScorer == null) {
                return null;
            }

            return new ProfileScorer(this, subQueryScorer);
        }

        @Override
        public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
            Explanation subQueryExpl = subQueryWeight.explain(context, doc);
            return subQueryExpl;

        }
    }

    static class ProfileScorer extends Scorer {

        private final Scorer scorer;
        private ProfileWeight profileWeight;

        private ProfileScorer(ProfileWeight w, Scorer scorer) throws IOException {
            super(w);
            this.scorer = scorer;
            this.profileWeight = w;
        }

        public void addTime(long time) {
            this.profileWeight.addTime(time);
        }

        @Override
        public int docID() {
            return scorer.docID();
        }

        @Override
        public int advance(int target) throws IOException {
            Stopwatch stopwatch = Stopwatch.createStarted();
            int id = scorer.advance(target);
            stopwatch.stop();
            addTime(stopwatch.elapsed(TimeUnit.MICROSECONDS));

            return id;
        }

        @Override
        public int nextDoc() throws IOException {
            Stopwatch stopwatch = Stopwatch.createStarted();
            int docId = scorer.nextDoc();
            stopwatch.stop();
            addTime(stopwatch.elapsed(TimeUnit.MICROSECONDS));

            return docId;
        }

        @Override
        public float score() throws IOException {
            Stopwatch stopwatch = Stopwatch.createStarted();
            float score = scorer.score();
            stopwatch.stop();
            addTime(stopwatch.elapsed(TimeUnit.MICROSECONDS));

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
    }

    public String toString(String field) {
        StringBuilder sb = new StringBuilder();

        // Currently only outputting the subquery's string.  This makes the ProfileQuery "invisible"
        // in explains/analyze, but makes the output much nicer for profiling
        sb.append(subQuery.toString(field));
        return sb.toString();
    }

    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        ProfileQuery other = (ProfileQuery) o;
        return this.getBoost() == other.getBoost() && this.subQuery.equals(other.subQuery);
    }

    // @TODO Do I just pick random features to make a hash?
    public int hashCode() {
        return subQuery.hashCode() + 31 *  Float.floatToIntBits(getBoost());
    }
}
