/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.cache.bloom.simple;

import org.apache.lucene.index.*;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.Unicode;
import org.elasticsearch.common.bloom.BloomFilter;
import org.elasticsearch.common.bloom.BloomFilterFactory;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeUnit;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.bloom.BloomCache;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.threadpool.ThreadPool;

import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class SimpleBloomCache extends AbstractIndexComponent implements BloomCache, SegmentReader.CoreClosedListener {

    private final ThreadPool threadPool;

    private final long maxSize;

    private final ConcurrentMap<Object, ConcurrentMap<String, BloomFilterEntry>> cache;

    private final Object creationMutex = new Object();

    @Inject
    public SimpleBloomCache(Index index, @IndexSettings Settings indexSettings, ThreadPool threadPool) {
        super(index, indexSettings);
        this.threadPool = threadPool;

        this.maxSize = indexSettings.getAsSize("index.cache.bloom.max_size", new SizeValue(500, SizeUnit.MEGA)).singles();
        this.cache = ConcurrentCollections.newConcurrentMap();
    }

    @Override
    public void close() throws ElasticSearchException {
        clear();
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public void onClose(SegmentReader owner) {
        clear(owner);
    }

    @Override
    public void clear(IndexReader reader) {
        ConcurrentMap<String, BloomFilterEntry> map = cache.remove(reader.getCoreCacheKey());
        // help soft/weak handling GC
        if (map != null) {
            map.clear();
        }
    }

    @Override
    public long sizeInBytes() {
        // the overhead of the map is not really relevant...
        long sizeInBytes = 0;
        for (ConcurrentMap<String, BloomFilterEntry> map : cache.values()) {
            for (BloomFilterEntry filter : map.values()) {
                sizeInBytes += filter.filter.sizeInBytes();
            }
        }
        return sizeInBytes;
    }

    @Override
    public long sizeInBytes(String fieldName) {
        long sizeInBytes = 0;
        for (ConcurrentMap<String, BloomFilterEntry> map : cache.values()) {
            BloomFilterEntry filter = map.get(fieldName);
            if (filter != null) {
                sizeInBytes += filter.filter.sizeInBytes();
            }
        }
        return sizeInBytes;
    }

    @Override
    public BloomFilter filter(IndexReader reader, String fieldName, boolean asyncLoad) {
        int currentNumDocs = reader.numDocs();
        if (currentNumDocs == 0) {
            return BloomFilter.EMPTY;
        }
        ConcurrentMap<String, BloomFilterEntry> fieldCache = cache.get(reader.getCoreCacheKey());
        if (fieldCache == null) {
            synchronized (creationMutex) {
                fieldCache = cache.get(reader.getCoreCacheKey());
                if (fieldCache == null) {
                    if (reader instanceof SegmentReader) {
                        ((SegmentReader) reader).addCoreClosedListener(this);
                    }
                    fieldCache = ConcurrentCollections.newConcurrentMap();
                    cache.put(reader.getCoreCacheKey(), fieldCache);
                }
            }
        }
        BloomFilterEntry filter = fieldCache.get(fieldName);
        if (filter == null) {
            synchronized (fieldCache) {
                filter = fieldCache.get(fieldName);
                if (filter == null) {
                    filter = new BloomFilterEntry(currentNumDocs, BloomFilter.NONE);
                    fieldCache.put(fieldName, filter);
                    // now, do the async load of it...
                    if (currentNumDocs < maxSize) {
                        filter.loading.set(true);
                        BloomFilterLoader loader = new BloomFilterLoader(reader, fieldName);
                        if (asyncLoad) {
                            threadPool.executor(ThreadPool.Names.CACHE).execute(loader);
                        } else {
                            loader.run();
                            filter = fieldCache.get(fieldName);
                        }
                    }
                }
            }
        }
        // if we too many deletes, we need to reload the bloom filter so it will be more effective
        if (filter.numDocs > 1000 && filter.numDocs < maxSize && (currentNumDocs / filter.numDocs) < 0.6) {
            if (filter.loading.compareAndSet(false, true)) {
                // do the async loading
                BloomFilterLoader loader = new BloomFilterLoader(reader, fieldName);
                if (asyncLoad) {
                    threadPool.executor(ThreadPool.Names.CACHE).execute(loader);
                } else {
                    loader.run();
                    filter = fieldCache.get(fieldName);
                }
            }
        }
        return filter.filter;
    }

    class BloomFilterLoader implements Runnable {
        private final IndexReader reader;
        private final String field;

        BloomFilterLoader(IndexReader reader, String field) {
            this.reader = reader;
            this.field = StringHelper.intern(field);
        }

        @SuppressWarnings({"StringEquality"})
        @Override
        public void run() {
            TermDocs termDocs = null;
            TermEnum termEnum = null;
            try {
                UnicodeUtil.UTF8Result utf8Result = new UnicodeUtil.UTF8Result();
                BloomFilter filter = BloomFilterFactory.getFilter(reader.numDocs(), 15);
                termDocs = reader.termDocs();
                termEnum = reader.terms(new Term(field));
                do {
                    Term term = termEnum.term();
                    if (term == null || term.field() != field) break;

                    // LUCENE MONITOR: 4.0, move to use bytes!
                    Unicode.fromStringAsUtf8(term.text(), utf8Result);
                    termDocs.seek(termEnum);
                    while (termDocs.next()) {
                        // when traversing, make sure to ignore deleted docs, so the key->docId will be correct
                        if (!reader.isDeleted(termDocs.doc())) {
                            filter.add(utf8Result.result, 0, utf8Result.length);
                        }
                    }
                } while (termEnum.next());
                ConcurrentMap<String, BloomFilterEntry> fieldCache = cache.get(reader.getCoreCacheKey());
                if (fieldCache != null) {
                    if (fieldCache.containsKey(field)) {
                        BloomFilterEntry filterEntry = new BloomFilterEntry(reader.numDocs(), filter);
                        filterEntry.loading.set(false);
                        fieldCache.put(field, filterEntry);
                    }
                }
            } catch (AlreadyClosedException e) {
                // ignore, we are getting closed
            } catch (ClosedChannelException e) {
                // ignore, we are getting closed
            } catch (Exception e) {
                // ignore failures that result from a closed reader...
                if (reader.getRefCount() > 0) {
                    logger.warn("failed to load bloom filter for [{}]", e, field);
                }
            } finally {
                try {
                    if (termDocs != null) {
                        termDocs.close();
                    }
                } catch (Exception e) {
                    // ignore
                }
                try {
                    if (termEnum != null) {
                        termEnum.close();
                    }
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    static class BloomFilterEntry {
        final int numDocs;
        final BloomFilter filter;
        final AtomicBoolean loading = new AtomicBoolean();

        public BloomFilterEntry(int numDocs, BloomFilter filter) {
            this.numDocs = numDocs;
            this.filter = filter;
        }
    }
}