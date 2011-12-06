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

package org.elasticsearch.index.search.nested;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.docset.DocSet;
import org.elasticsearch.common.lucene.docset.DocSets;
import org.elasticsearch.common.lucene.docset.FixedBitDocSet;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetCollector;

import java.io.IOException;

/**
 * A collector that accepts parent docs, and calls back the collect on child docs of that parent.
 */
public class NestedChildrenCollector extends FacetCollector {

    private final FacetCollector collector;

    private final Filter parentFilter;

    private final Filter childFilter;

    private DocSet childDocs;

    private FixedBitSet parentDocs;

    private IndexReader currentReader;

    public NestedChildrenCollector(FacetCollector collector, Filter parentFilter, Filter childFilter) {
        this.collector = collector;
        this.parentFilter = parentFilter;
        this.childFilter = childFilter;
    }

    @Override
    public Facet facet() {
        return collector.facet();
    }

    @Override
    public void setFilter(Filter filter) {
        // delegate the facet_filter to the children
        collector.setFilter(filter);
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        collector.setScorer(scorer);
    }

    @Override
    public void setNextReader(IndexReader reader, int docBase) throws IOException {
        collector.setNextReader(reader, docBase);
        currentReader = reader;
        childDocs = DocSets.convert(reader, childFilter.getDocIdSet(reader));
        parentDocs = ((FixedBitDocSet) parentFilter.getDocIdSet(reader)).set();
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return collector.acceptsDocsOutOfOrder();
    }

    @Override
    public void collect(int parentDoc) throws IOException {
        if (parentDoc == 0) {
            return;
        }
        int prevParentDoc = parentDocs.prevSetBit(parentDoc - 1);
        for (int i = (parentDoc - 1); i > prevParentDoc; i--) {
            if (!currentReader.isDeleted(i) && childDocs.get(i)) {
                collector.collect(i);
            }
        }
    }
}