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

package org.elasticsearch.search.internal;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.docset.ContextDocIdSet;
import org.elasticsearch.common.lucene.search.XCollector;
import org.elasticsearch.index.cache.docset.DocSetCache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class DocIdSetCollector extends XCollector implements Releasable {

    private final DocSetCache docSetCache;
    private final Collector collector;

    private final List<ContextDocIdSet> docSets;
    private boolean currentHasDocs;
    private ContextDocIdSet currentContext;
    private FixedBitSet currentSet;

    public DocIdSetCollector(DocSetCache docSetCache, Collector collector) {
        this.docSetCache = docSetCache;
        this.collector = collector;
        this.docSets = new ArrayList<>();
    }

    public List<ContextDocIdSet> docSets() {
        return docSets;
    }

    public void close() {
        for (ContextDocIdSet docSet : docSets) {
            docSetCache.release(docSet);
        }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        collector.setScorer(scorer);
    }

    @Override
    public void collect(int doc) throws IOException {
        collector.collect(doc);
        currentHasDocs = true;
        currentSet.set(doc);
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        collector.setNextReader(context);
        if (currentContext != null) {
            if (currentHasDocs) {
                docSets.add(currentContext);
            } else {
                docSetCache.release(currentContext);
            }
        }
        currentContext = docSetCache.obtain(context);
        currentSet = (FixedBitSet) currentContext.docSet;
        currentHasDocs = false;
    }

    @Override
    public void postCollection() throws IOException {
        if (collector instanceof XCollector) {
            ((XCollector) collector).postCollection();
        }
        if (currentContext != null) {
            if (currentHasDocs) {
                docSets.add(currentContext);
            } else {
                docSetCache.release(currentContext);
            }
            currentContext = null;
            currentSet = null;
            currentHasDocs = false;
        }
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }
}
