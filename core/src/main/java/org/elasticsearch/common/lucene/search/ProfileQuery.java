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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;

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

    private long rewriteTime = 0;
    private long executionTime = 0;
    private long totalTime = 0;

    private Map<Query, Long> timings;

    private Stopwatch stopwatch;

    public enum Timing {
        REWRITE, EXECUTION, ALL
    }

    public ProfileQuery(Query subQuery) {
        this();
        setSubQuery(subQuery);
        timings = new HashMap<>();
    }

    public ProfileQuery() {
        this.stopwatch = Stopwatch.createUnstarted();
    }

    public Query subQuery() {
        return subQuery;
    }

    public void setSubQuery(Query subQuery) {
        this.subQuery = subQuery;
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
                throw new IllegalArgumentException("Must setTime for either REWRITE or EXECUTION timing.");
            default:
                throw new IllegalArgumentException("Must setTime for either REWRITE or EXECUTION timing.");
        }
    }

    public void startTime() {
        stopwatch.start();
    }

    public void stopAndRecordTime(Timing timing, Query query) {
        stopwatch.stop();
        long time = stopwatch.elapsed(TimeUnit.MICROSECONDS);
        stopwatch.reset();

        timings.put(query, time);

        switch (timing) {
            case REWRITE:
                rewriteTime += time;
                break;
            case EXECUTION:
                executionTime += time;
                break;
            case ALL:
                throw new IllegalArgumentException("Must setTime for either REWRITE or EXECUTION timing.");
            default:
                throw new IllegalArgumentException("Must setTime for either REWRITE or EXECUTION timing.");
        }
    }

    /*
    @Override
    public void extractTerms(Set<Term> terms) {
        super(terms);
        //subQuery.extractTerms(terms);
    }
    */

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
}