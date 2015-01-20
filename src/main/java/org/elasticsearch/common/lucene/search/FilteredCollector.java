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
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.docset.DocIdSets;

import java.io.IOException;

/**
 *
 */
public class FilteredCollector implements XCollector {

    private final Collector collector;
    private final Filter filter;

    public FilteredCollector(Collector collector, Filter filter) {
        this.collector = collector;
        this.filter = filter;
    }

    @Override
    public void postCollection() throws IOException {
        if (collector instanceof XCollector) {
            ((XCollector) collector).postCollection();
        }
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        final DocIdSet set = filter.getDocIdSet(context, null);
        final LeafCollector in = collector.getLeafCollector(context);
        final Bits bits = set == null ? null : set.bits();

        if (bits != null) {
            // the filter supports random-access
            return new FilterLeafCollector(in) {
                public void collect(int doc) throws IOException {
                    if (bits.get(doc)) {
                        in.collect(doc);
                    }
                }
            };
        }

        // No random-access support, use the iterator and force in-order scoring
        final DocIdSetIterator iterator;
        if (DocIdSets.isEmpty(set)) {
            iterator = null;
        } else {
            // DIS.iterator might still return null here
            iterator = set.iterator();
        }

        if (iterator == null) {
            return new FilterLeafCollector(in) {
                @Override
                public void collect(int doc) throws IOException {
                    // no-op
                }
            };
        }

        return new FilterLeafCollector(in) {
            @Override
            public void collect(int doc) throws IOException {
                final int itDoc = iterator.docID();
                if (itDoc > doc) {
                    return;
                } else if (itDoc < doc) {
                    if (iterator.advance(doc) == doc) {
                        in.collect(doc);
                    }
                } else {
                    in.collect(doc);
                }
            }
        };
    }

}