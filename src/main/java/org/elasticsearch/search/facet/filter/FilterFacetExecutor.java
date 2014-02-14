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

package org.elasticsearch.search.facet.filter;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.docset.AndDocIdSet;
import org.elasticsearch.common.lucene.docset.ContextDocIdSet;
import org.elasticsearch.common.lucene.docset.DocIdSets;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class FilterFacetExecutor extends FacetExecutor {

    private final Filter filter;

    long count = -1;

    public FilterFacetExecutor(Filter filter) {
        this.filter = filter;
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
        return new InternalFilterFacet(facetName, count);
    }

    class Post extends FacetExecutor.Post {

        @Override
        public void executePost(List<ContextDocIdSet> docSets) throws IOException {
            int count = 0;
            for (ContextDocIdSet docSet : docSets) {
                DocIdSet filteredDocIdSet = filter.getDocIdSet(docSet.context, docSet.context.reader().getLiveDocs());
                if (filteredDocIdSet == null || docSet.docSet == null) {
                    continue;
                }
                DocIdSetIterator iter = new AndDocIdSet(new DocIdSet[]{docSet.docSet, filteredDocIdSet}).iterator();
                while (iter.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    count++;
                }
            }
            FilterFacetExecutor.this.count = count;
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
            FilterFacetExecutor.this.count = count;
        }
    }
}
