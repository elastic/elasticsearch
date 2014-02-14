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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.MultiTermQueryWrapperFilter;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.automaton.RegExp;

import java.io.IOException;

/**
 * A lazy regexp filter which only builds the automaton on the first call to {@link #getDocIdSet(AtomicReaderContext, Bits)}.
 * It is not thread safe (so can't be applied on multiple segments concurrently)
 */
public class RegexpFilter extends Filter {

    private final Term term;
    private final int flags;

    // use delegation here to support efficient implementation of equals & hashcode for this
    // filter (as it will be used as the filter cache key)
    private final InternalFilter filter;

    public RegexpFilter(Term term) {
        this(term, RegExp.ALL);
    }

    public RegexpFilter(Term term, int flags) {
        filter = new InternalFilter(term, flags);
        this.term = term;
        this.flags = flags;
    }

    public String field() {
        return term.field();
    }

    public String regexp() {
        return term.text();
    }

    public int flags() {
        return flags;
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        return filter.getDocIdSet(context, acceptDocs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        org.elasticsearch.common.lucene.search.RegexpFilter that = (org.elasticsearch.common.lucene.search.RegexpFilter) o;

        if (flags != that.flags) return false;
        if (term != null ? !term.equals(that.term) : that.term != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = term != null ? term.hashCode() : 0;
        result = 31 * result + flags;
        return result;
    }

    @Override
    public String toString() {
        // todo should we also show the flags?
        return term.field() + ":" + term.text();
    }

    static class InternalFilter extends MultiTermQueryWrapperFilter<RegexpQuery> {

        public InternalFilter(Term term) {
            super(new RegexpQuery(term));
        }

        public InternalFilter(Term term, int flags) {
            super(new RegexpQuery(term, flags));
        }
    }

}
