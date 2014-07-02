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

import java.io.IOException;
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
    private long time = 0;

    private String className;
    private String details;

    public ProfileFilter(Filter filter) {
        this.subFilter = filter;
        this.setClassName(filter.getClass().getSimpleName());
        this.setDetails(filter.toString());
    }

    public Filter subFilter() {
        return this.subFilter;
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
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        DocIdSet idSet = this.subFilter.getDocIdSet(context, acceptDocs);
        stopwatch.stop();
        addTime(stopwatch.elapsed(TimeUnit.MICROSECONDS));

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
