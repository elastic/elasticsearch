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
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.elasticsearch.ElasticsearchIllegalArgumentException;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

/**
 * This class times the execution of the subfilter that it wraps.  Timing includes:
 *
 *  - ProfileFilter.getDocIdSet
 *
 *  A ProfileFilter maintains it's own timing independent of the rest of the query.
 *  It must be later aggregated together using Profile.collapse
 */
public class ProfileFilter extends Filter implements ProfileComponent {

    private Filter subFilter;
    ProfileQuery parentQuery = null;

    private long rewriteTime = 0;
    private long executionTime = 0;

    private String className;
    private String details;
    private Stopwatch stopwatch;

    public ProfileFilter(Filter filter) {
        this.subFilter = filter;
        this.setClassName(filter.getClass().getSimpleName());
        this.setDetails(filter.toString());
        this.stopwatch = Stopwatch.createUnstarted();
    }

    public Filter subFilter() {
        return this.subFilter;
    }

    public ProfileQuery parentQuery() {
        return parentQuery;
    }

    public void setParentQuery(ProfileQuery parent) {
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

        //OH GOD WHY
        Method[] methods = this.subFilter.getClass().getMethods();
        for (Method m : methods) {
            if (m.getName().equals("clauses") || m.getName().equals("getQuery") || m.getName().equals("getFilter")) {
                return;
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
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        stopwatch.start();
        DocIdSet idSet = this.subFilter.getDocIdSet(context, acceptDocs);
        stopwatch.stop();
        addTime(Timing.EXECUTION, stopwatch.elapsed(TimeUnit.MICROSECONDS));
        stopwatch.reset();

        return idSet;
    }

    @Override
    public int hashCode() {
        int hash = 8;
        hash = 31 * hash + (this.subFilter.hashCode());
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if ((obj == null) || (obj.getClass() != this.getClass()))
            return false;

        ProfileFilter other = (ProfileFilter) obj;
        return this.subFilter.equals(other.subFilter());
    }

    @Override
    public String toString() {
        return this.subFilter.toString();
    }


}
