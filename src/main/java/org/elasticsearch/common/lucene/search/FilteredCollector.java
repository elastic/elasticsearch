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
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.docset.DocIdSets;

import java.io.IOException;

/**
 *
 */
public class FilteredCollector extends XCollector {

    private final Collector collector;

    private final Filter filter;

    private Bits docSet;

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
    public void setScorer(Scorer scorer) throws IOException {
        collector.setScorer(scorer);
    }

    @Override
    public void collect(int doc) throws IOException {
        if (docSet.get(doc)) {
            collector.collect(doc);
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        collector.setNextReader(context);
        docSet = DocIdSets.toSafeBits(context.reader(), filter.getDocIdSet(context, null));
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return collector.acceptsDocsOutOfOrder();
    }
}