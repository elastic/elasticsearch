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

package org.elasticsearch.index.cache.filter.weak;

import org.apache.lucene.search.Filter;
import org.elasticsearch.common.base.Objects;
import org.elasticsearch.common.collect.MapEvictionListener;
import org.elasticsearch.common.collect.MapMaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.docset.DocSet;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.filter.support.AbstractConcurrentMapFilterCache;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A weak reference based filter cache that has weak keys on the <tt>IndexReader</tt>.
 *
 * @author kimchy (shay.banon)
 */
public class WeakFilterCache extends AbstractConcurrentMapFilterCache implements MapEvictionListener<Filter, DocSet> {

    private final IndexSettingsService indexSettingsService;

    private volatile int maxSize;
    private volatile TimeValue expire;

    private final AtomicLong evictions = new AtomicLong();
    private AtomicLong memEvictions;

    private final ApplySettings applySettings = new ApplySettings();

    @Inject public WeakFilterCache(Index index, @IndexSettings Settings indexSettings, IndexSettingsService indexSettingsService) {
        super(index, indexSettings);
        this.indexSettingsService = indexSettingsService;
        this.maxSize = indexSettings.getAsInt("index.cache.filter.max_size", componentSettings.getAsInt("max_size", -1));
        this.expire = indexSettings.getAsTime("index.cache.filter.expire", componentSettings.getAsTime("expire", null));
        logger.debug("using [weak] filter cache with max_size [{}], expire [{}]", maxSize, expire);

        indexSettingsService.addListener(applySettings);
    }

    @Override public void close() {
        indexSettingsService.removeListener(applySettings);
        super.close();
    }

    @Override protected ConcurrentMap<Object, ReaderValue> buildCache() {
        memEvictions = new AtomicLong(); // we need to init it here, since its called from the super constructor
        // better to have weak on the whole ReaderValue, simpler on the GC to clean it
        MapMaker mapMaker = new MapMaker().weakKeys().softValues();
        mapMaker.evictionListener(new CacheMapEvictionListener(memEvictions));
        return mapMaker.makeMap();
    }

    @Override protected ConcurrentMap<Filter, DocSet> buildFilterMap() {
        MapMaker mapMaker = new MapMaker();
        if (maxSize != -1) {
            mapMaker.maximumSize(maxSize);
        }
        if (expire != null) {
            mapMaker.expireAfterAccess(expire.nanos(), TimeUnit.NANOSECONDS);
        }
        mapMaker.evictionListener(this);
        return mapMaker.makeMap();
    }

    @Override public String type() {
        return "weak";
    }

    @Override public long evictions() {
        return evictions.get();
    }

    @Override public long memEvictions() {
        return memEvictions.get();
    }

    @Override public void onEviction(Filter filter, DocSet docSet) {
        evictions.incrementAndGet();
    }

    class ApplySettings implements IndexSettingsService.Listener {
        @Override public void onRefreshSettings(Settings settings) {
            int maxSize = settings.getAsInt("index.cache.filter.max_size", WeakFilterCache.this.maxSize);
            TimeValue expire = settings.getAsTime("index.cache.filter.expire", WeakFilterCache.this.expire);
            boolean changed = false;
            if (maxSize != WeakFilterCache.this.maxSize) {
                logger.info("updating index.cache.filter.max_size from [{}] to [{}]", WeakFilterCache.this.maxSize, maxSize);
                changed = true;
                WeakFilterCache.this.maxSize = maxSize;
            }
            if (!Objects.equal(expire, WeakFilterCache.this.expire)) {
                logger.info("updating index.cache.filter.expire from [{}] to [{}]", WeakFilterCache.this.expire, expire);
                changed = true;
                WeakFilterCache.this.expire = expire;
            }
            if (changed) {
                clear();
            }
        }
    }
}