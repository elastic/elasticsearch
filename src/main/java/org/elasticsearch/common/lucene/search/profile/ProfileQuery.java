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
import org.elasticsearch.ElasticsearchIllegalArgumentException;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
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
    ProfileQuery parentQuery = null;

    private long rewriteTime = 0;
    private long executionTime = 0;

    private String className;
    private String details;
    private Stopwatch stopwatch;

    public ProfileQuery(Query subQuery) {
        this();
        setSubQuery(subQuery);
    }

    public ProfileQuery() {
        this.stopwatch = Stopwatch.createUnstarted();
    }

    public Query subQuery() {
        return subQuery;
    }

    public void setSubQuery(Query subQuery) {
        this.subQuery = subQuery;
        this.setClassName(subQuery.getClass().getSimpleName());
        this.setDetails(subQuery.toString());
    }

    public ProfileQuery parentQuery() {
        return parentQuery;
    }

    public void setParentQuery(ProfileQuery parent) {
        // Only set a parent if we don't already have one.  This is important because some rewrites
        // (like a multi-nested, single-clause bool) will "collapse" the tree.  If we overwrite the parent
        // each time, the terminal query would report to the top-most compound, which is wrong
        if (this.parentQuery == null) {
            this.parentQuery = parent;
        }
    }

    public long time(Timing timing) {
        switch (timing) {
            case REWRITE:
                return rewriteTime;
            case EXECUTION:
                return executionTime;
            case ALL:
                return rewriteTime + executionTime;
            default:
                return rewriteTime + executionTime;
        }
    }

    public void setTime(Timing timing, long time) {
        switch (timing) {
            case REWRITE:
                rewriteTime = time;
                break;
            case EXECUTION:
                executionTime = time;
                break;
            case ALL:
                throw new ElasticsearchIllegalArgumentException("Must setTime for either REWRITE or EXECUTION timing.");
            default:
                throw new ElasticsearchIllegalArgumentException("Must setTime for either REWRITE or EXECUTION timing.");
        }
    }

    public void addTime(Timing timing, long time) {

        if (timing.equals(Timing.REWRITE)) {
            //OH GOD WHY
            Method[] methods = this.subQuery.getClass().getMethods();
            for (Method m : methods) {
                if (m.getName().equals("clauses") || m.getName().equals("getQuery") || m.getName().equals("getFilter")) {
                    return;
                }
            }
        }


        if (parentQuery != null) {
            addParentTime(timing, time);
        } else {
            addLocalTime(timing, time);
        }
    }

    private void addLocalTime(Timing timing, long time) {
        switch (timing) {
            case REWRITE:
                rewriteTime += time;
                break;
            case EXECUTION:
                executionTime += time;
                break;
            case ALL:
                throw new ElasticsearchIllegalArgumentException("Must addTime for either REWRITE or EXECUTION timing.");
            default:
                throw new ElasticsearchIllegalArgumentException("Must addTime for either REWRITE or EXECUTION timing.");
        }
    }

    private void addParentTime(Timing timing, long time) {
        parentQuery.addTime(timing, time);
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
        stopwatch.start();
        Query rewrittenQuery = subQuery.rewrite(reader);
        stopwatch.stop();

        addTime(Timing.REWRITE, stopwatch.elapsed(TimeUnit.MICROSECONDS));
        stopwatch.reset();

        if (rewrittenQuery == subQuery) {
            return this;
        }

        // The rewriting process can potentially add many new nested components
        // Perform a walk of the rewritten query to wrap all the new parts

        // We instantiate the walker with a pointer to `this`, because these new components will not
        // be part of the original query, and thus will be "hidden" unless we redirect their timings.
        // When invoked with a ProfileQuery param, the Visitor will inject the reference into all
        // newly created ProfileQueries
        ProfileQueryVisitor walker = new ProfileQueryVisitor(this);
        ProfileQuery wrappedRewrittenQuery = (ProfileQuery) walker.apply(rewrittenQuery);

        if (wrappedRewrittenQuery == rewrittenQuery) {
            return rewrittenQuery;
        } else {
            wrappedRewrittenQuery.setParentQuery(this);
            return wrappedRewrittenQuery;
        }

    }

    @Override
    public void extractTerms(Set<Term> terms) {
        subQuery.extractTerms(terms);
    }

    /** Create a shallow copy of us -- used in rewriting if necessary
     * @return a copy of us (but reuse, don't copy, our subqueries) */
    @Override
    public Query clone() {
        ProfileQuery p = (ProfileQuery)super.clone();
        p.subQuery = this.subQuery;
        p.setTime(Timing.REWRITE, time(Timing.REWRITE));
        p.setTime(Timing.EXECUTION, time(Timing.EXECUTION));
        return p;
    }


    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
        stopwatch.start();
        Weight subQueryWeight = subQuery.createWeight(searcher);
        stopwatch.stop();
        addTime(Timing.EXECUTION, stopwatch.elapsed(TimeUnit.MICROSECONDS));
        stopwatch.reset();

        return new ProfileWeight(subQueryWeight, this);
    }

    class ProfileWeight extends Weight {

        final Weight subQueryWeight;
        private ProfileQuery profileQuery;
        private Stopwatch stopwatch;

        public ProfileWeight(Weight subQueryWeight, ProfileQuery profileQuery) throws IOException {
            this.subQueryWeight = subQueryWeight;
            this.profileQuery = profileQuery;
            this.stopwatch = Stopwatch.createUnstarted();
        }

        public Query getQuery() {
            return ProfileQuery.this;
        }

        public void addTime(long time) {
            this.profileQuery.addTime(Timing.EXECUTION, time);
        }

        @Override
        public float getValueForNormalization() throws IOException {
            stopwatch.start();
            float sum = subQueryWeight.getValueForNormalization();
            stopwatch.stop();
            addTime(stopwatch.elapsed(TimeUnit.MICROSECONDS));
            stopwatch.reset();

            return sum;
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
            stopwatch.start();
            subQueryWeight.normalize(norm, topLevelBoost * getBoost());
            stopwatch.stop();
            addTime(stopwatch.elapsed(TimeUnit.MICROSECONDS));
            stopwatch.reset();
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
        private Stopwatch stopwatch;

        private ProfileScorer(ProfileWeight w, Scorer scorer) throws IOException {
            super(w);
            this.scorer = scorer;
            this.profileWeight = w;
            this.stopwatch = Stopwatch.createUnstarted();
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
            stopwatch.start();
            int id = scorer.advance(target);
            stopwatch.stop();
            addTime(stopwatch.elapsed(TimeUnit.MICROSECONDS));
            stopwatch.reset();
            return id;
        }

        @Override
        public int nextDoc() throws IOException {
            stopwatch.start();
            int docId = scorer.nextDoc();
            stopwatch.stop();
            addTime(stopwatch.elapsed(TimeUnit.MICROSECONDS));
            stopwatch.reset();

            return docId;
        }

        @Override
        public float score() throws IOException {
            stopwatch.start();
            float score = scorer.score();
            stopwatch.stop();
            addTime(stopwatch.elapsed(TimeUnit.MICROSECONDS));
            stopwatch.reset();

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
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ProfileQuery other = (ProfileQuery) o;
        return this.className.equals(other.className)
                && this.details.equals(other.details)
                && this.subQuery.equals(other.subQuery)
                && Float.floatToIntBits(this.getBoost()) == Float.floatToIntBits(other.getBoost());
    }

    public int hashCode() {
        final int prime = 31;
        int result = prime * 19 + this.className.hashCode();
        result = prime * result + this.details.hashCode();
        result = prime * result + this.subQuery.hashCode();
        result = prime * result + Float.floatToIntBits(getBoost());
        return result;
    }
}
