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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.docset.DocIdSets;
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

    private Bits childDocs;

    private FixedBitSet parentDocs;

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
    public void setNextReader(AtomicReaderContext context) throws IOException {
        collector.setNextReader(context);
        // Can use null as acceptedDocs here, since only live doc ids are being pushed to collect method.
        DocIdSet docIdSet = parentFilter.getDocIdSet(context, null);
        // Im ES if parent is deleted, then also the children are deleted. Therefore acceptedDocs can also null here.
        childDocs = DocIdSets.toSafeBits(context.reader(), childFilter.getDocIdSet(context, null));
        if (docIdSet == null) {
            parentDocs = null;
        } else {
            parentDocs = (FixedBitSet) docIdSet;
        }
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return collector.acceptsDocsOutOfOrder();
    }

    @Override
    public void collect(int parentDoc) throws IOException {
        if (parentDoc == 0 || parentDocs == null) {
            return;
        }
        int prevParentDoc = parentDocs.prevSetBit(parentDoc - 1);
        for (int i = (parentDoc - 1); i > prevParentDoc; i--) {
            if (childDocs.get(i)) {
                collector.collect(i);
            }
        }
    }
}