/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.search.facet.terms;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.support.AbstractFacetCollector;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class IndexNameFacetCollector extends AbstractFacetCollector {

    private final String indexName;

    private final InternalTermsFacet.ComparatorType comparatorType;

    private final int size;

    private int count = 0;

    public IndexNameFacetCollector(String facetName, String indexName, TermsFacet.ComparatorType comparatorType, int size) {
        super(facetName);
        this.indexName = indexName;
        this.comparatorType = comparatorType;
        this.size = size;
    }

    @Override protected void doSetNextReader(IndexReader reader, int docBase) throws IOException {
    }

    @Override protected void doCollect(int doc) throws IOException {
        count++;
    }

    @Override public Facet facet() {
        return new InternalTermsFacet(facetName, "_index", comparatorType, size, Sets.newHashSet(new TermsFacet.Entry(indexName, count)));
    }
}
