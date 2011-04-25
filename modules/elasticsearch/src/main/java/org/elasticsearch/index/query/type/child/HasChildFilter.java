/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query.type.child;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.OpenBitSet;
import org.elasticsearch.search.internal.ScopePhase;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class HasChildFilter extends Filter implements ScopePhase.CollectorPhase {

    private Query query;

    private String scope;

    private String parentType;

    private String childType;

    private final SearchContext searchContext;

    private Map<Object, OpenBitSet> parentDocs;

    public HasChildFilter(Query query, String scope, String childType, String parentType, SearchContext searchContext) {
        this.query = query;
        this.scope = scope;
        this.parentType = parentType;
        this.childType = childType;
        this.searchContext = searchContext;
    }

    @Override public Query query() {
        return query;
    }

    @Override public boolean requiresProcessing() {
        return parentDocs == null;
    }

    @Override public Collector collector() {
        return new ChildCollector(parentType, searchContext);
    }

    @Override public void processCollector(Collector collector) {
        this.parentDocs = ((ChildCollector) collector).parentDocs();
    }

    @Override public String scope() {
        return this.scope;
    }

    @Override public void clear() {
        parentDocs = null;
    }

    @Override public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        // ok to return null
        return parentDocs.get(reader.getCoreCacheKey());
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("child_filter[").append(childType).append("/").append(parentType).append("](").append(query).append(')');
        return sb.toString();
    }
}
