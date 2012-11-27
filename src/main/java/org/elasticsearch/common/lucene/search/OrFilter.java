/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.docset.OrDocIdSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class OrFilter extends Filter {

    private final List<? extends Filter> filters;

    public OrFilter(List<? extends Filter> filters) {
        this.filters = filters;
    }

    public List<? extends Filter> filters() {
        return filters;
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        if (filters.size() == 1) {
            return filters.get(0).getDocIdSet(context, acceptDocs);
        }
        List<DocIdSet> sets = new ArrayList<DocIdSet>(filters.size());
        for (int i = 0; i < filters.size(); i++) {
            DocIdSet set = filters.get(i).getDocIdSet(context, acceptDocs);
            if (DocIdSets.isEmpty(set)) { // none matching for this filter, continue
                continue;
            }
            sets.add(set);
        }
        if (sets.size() == 0) {
            return null;
        }
        if (sets.size() == 1) {
            return sets.get(0);
        }
        return new OrDocIdSet(sets.toArray(new DocIdSet[sets.size()]));
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (null == filters ? 0 : filters.hashCode());
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if ((obj == null) || (obj.getClass() != this.getClass()))
            return false;

        OrFilter other = (OrFilter) obj;
        return equalFilters(filters, other.filters);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (Filter filter : filters) {
            if (builder.length() > 0) {
                builder.append(' ');
            }
            builder.append(filter);
        }
        return builder.toString();
    }

    private boolean equalFilters(List<? extends Filter> filters1, List<? extends Filter> filters2) {
        return (filters1 == filters2) || ((filters1 != null) && filters1.equals(filters2));
    }
}