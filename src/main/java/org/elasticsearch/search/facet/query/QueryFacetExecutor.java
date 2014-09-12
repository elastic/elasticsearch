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

package org.elasticsearch.search.facet.query;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.docset.AndDocIdSet;
import org.elasticsearch.common.lucene.docset.ContextDocIdSet;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.lucene.search.XFilteredQuery;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class QueryFacetExecutor extends FacetExecutor {

    private final Query query;
    private final Filter filter;

    // default to not initialized
    long count = -1;

    public QueryFacetExecutor(Query query) {
        this.query = query;
        Filter possibleFilter = extractFilterIfApplicable(query);
        if (possibleFilter != null) {
            this.filter = possibleFilter;
        } else {
            this.filter = Queries.wrap(query, null);
        }
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public Post post() {
        return new Post();
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        return new InternalQueryFacet(facetName, count);
    }

    class Post extends FacetExecutor.Post {

        @Override
        public void executePost(List<ContextDocIdSet> docSets) throws IOException {
            int count = 0;
            for (ContextDocIdSet entry : docSets) {
                DocIdSet filteredDocIdSet = filter.getDocIdSet(entry.context, entry.context.reader().getLiveDocs());
                if (filteredDocIdSet == null || entry.docSet == null) {
                    continue;
                }
                DocIdSetIterator iter = new AndDocIdSet(new DocIdSet[]{entry.docSet, filteredDocIdSet}).iterator();
                while (iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    count++;
                }
            }
            QueryFacetExecutor.this.count = count;
        }
    }

    class Collector extends FacetExecutor.Collector {

        private long count = 0;
        private Bits bits;

        @Override
        public void collect(int doc) throws IOException {
            if (bits.get(doc)) {
                count++;
            }
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            bits = DocIdSets.toSafeBits(context.reader(), filter.getDocIdSet(context, context.reader().getLiveDocs()));
        }

        @Override
        public void postCollection() {
            bits = null;
            QueryFacetExecutor.this.count = count;
        }
    }

    /**
     * If its a filtered query with a match all, then we just need the inner filter.
     */
    private Filter extractFilterIfApplicable(Query query) {
        if (query instanceof XFilteredQuery) {
            XFilteredQuery fQuery = (XFilteredQuery) query;
            if (Queries.isConstantMatchAllQuery(fQuery.getQuery())) {
                return fQuery.getFilter();
            }
        } else if (query instanceof XConstantScoreQuery) {
            return ((XConstantScoreQuery) query).getFilter();
        } else if (query instanceof ConstantScoreQuery) {
            ConstantScoreQuery constantScoreQuery = (ConstantScoreQuery) query;
            if (constantScoreQuery.getFilter() != null) {
                return constantScoreQuery.getFilter();
            }
        }
        return null;
    }
}
