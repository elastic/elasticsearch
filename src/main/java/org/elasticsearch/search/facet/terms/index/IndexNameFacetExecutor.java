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

package org.elasticsearch.search.facet.terms.index;

import com.google.common.collect.Sets;
import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.strings.InternalStringTermsFacet;

import java.io.IOException;

/**
 *
 */
public class IndexNameFacetExecutor extends FacetExecutor {

    private final String indexName;
    private final InternalStringTermsFacet.ComparatorType comparatorType;
    private final int size;

    private int count = 0;

    public IndexNameFacetExecutor(String indexName, TermsFacet.ComparatorType comparatorType, int size) {
        this.indexName = indexName;
        this.comparatorType = comparatorType;
        this.size = size;
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        return new InternalStringTermsFacet(facetName, comparatorType, size, Sets.newHashSet(new InternalStringTermsFacet.TermEntry(indexName, count)), 0, count);
    }

    class Collector extends FacetExecutor.Collector {

        private int count;

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
        }

        @Override
        public void collect(int doc) throws IOException {
            count++;
        }

        @Override
        public void postCollection() {
            IndexNameFacetExecutor.this.count = count;
        }
    }
}
