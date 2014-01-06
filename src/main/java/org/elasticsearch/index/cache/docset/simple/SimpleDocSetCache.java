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

package org.elasticsearch.index.cache.docset.simple;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.docset.ContextDocIdSet;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.docset.DocSetCache;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.Queue;
import java.util.concurrent.ConcurrentMap;

/**
 */
public class SimpleDocSetCache extends AbstractIndexComponent implements DocSetCache, SegmentReader.CoreClosedListener {

    private final ConcurrentMap<Object, Queue<FixedBitSet>> cache;

    @Inject
    public SimpleDocSetCache(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
        this.cache = ConcurrentCollections.newConcurrentMap();
    }

    @Override
    public void onClose(Object coreCacheKey) {
        cache.remove(coreCacheKey);
    }

    @Override
    public void clear(String reason) {
        cache.clear();
    }

    @Override
    public void clear(IndexReader reader) {
        cache.remove(reader.getCoreCacheKey());
    }

    @Override
    public ContextDocIdSet obtain(AtomicReaderContext context) {
        Queue<FixedBitSet> docIdSets = cache.get(context.reader().getCoreCacheKey());
        if (docIdSets == null) {
            if (context.reader() instanceof SegmentReader) {
                ((SegmentReader) context.reader()).addCoreClosedListener(this);
            }
            cache.put(context.reader().getCoreCacheKey(), ConcurrentCollections.<FixedBitSet>newQueue());
            return new ContextDocIdSet(context, new FixedBitSet(context.reader().maxDoc()));
        }
        FixedBitSet docIdSet = docIdSets.poll();
        if (docIdSet == null) {
            docIdSet = new FixedBitSet(context.reader().maxDoc());
        } else {
            docIdSet.clear(0, docIdSet.length());
        }
        return new ContextDocIdSet(context, docIdSet);
    }

    @Override
    public void release(ContextDocIdSet docSet) {
        Queue<FixedBitSet> docIdSets = cache.get(docSet.context.reader().getCoreCacheKey());
        if (docIdSets != null) {
            docIdSets.add((FixedBitSet) docSet.docSet);
        }
    }
}
