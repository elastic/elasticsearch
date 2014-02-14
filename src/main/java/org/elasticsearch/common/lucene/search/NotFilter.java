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
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.docset.AllDocIdSet;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.docset.NotDocIdSet;

import java.io.IOException;

/**
 *
 */
public class NotFilter extends Filter {

    private final Filter filter;

    public NotFilter(Filter filter) {
        this.filter = filter;
    }

    public Filter filter() {
        return filter;
    }

    @Override
    public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) throws IOException {
        DocIdSet set = filter.getDocIdSet(context, acceptDocs);
        if (DocIdSets.isEmpty(set)) {
            return new AllDocIdSet(context.reader().maxDoc());
        }
        return new NotDocIdSet(set, context.reader().maxDoc());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NotFilter notFilter = (NotFilter) o;
        return !(filter != null ? !filter.equals(notFilter.filter) : notFilter.filter != null);
    }

    @Override
    public String toString() {
        return "NotFilter(" + filter + ")";
    }

    @Override
    public int hashCode() {
        return filter != null ? filter.hashCode() : 0;
    }
}
