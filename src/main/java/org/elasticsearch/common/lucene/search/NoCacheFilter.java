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
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;

import java.io.IOException;

/**
 * A marker interface for {@link org.apache.lucene.search.Filter} denoting the filter
 * as one that should not be cached, ever.
 */
public abstract class NoCacheFilter extends Filter {

    private static final class NoCacheFilterWrapper extends NoCacheFilter {
        private final Filter delegate;
        private NoCacheFilterWrapper(Filter delegate) {
            this.delegate = delegate;
        }

        @Override
        public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
            return delegate.getDocIdSet(context, acceptDocs);
        }

        @Override
        public int hashCode() {
            return delegate.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj instanceof NoCacheFilterWrapper) {
                return delegate.equals(((NoCacheFilterWrapper)obj).delegate);
            }
            return false;
        }

        @Override
        public String toString(String field) {

            return "no_cache(" + delegate + ")";
        }

    }

    /**
     * Wraps a filter in a NoCacheFilter or returns it if it already is a NoCacheFilter.
     */
    public static Filter wrap(Filter filter) {
        if (filter instanceof NoCacheFilter) {
            return filter;
        }
        return new NoCacheFilterWrapper(filter);
    }
}