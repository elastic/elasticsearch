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

package org.elasticsearch.index.cache.filter.soft;

import org.apache.lucene.search.Filter;
import org.elasticsearch.common.collect.MapEvictionListener;
import org.elasticsearch.common.collect.MapMaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.docset.DocSet;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.filter.support.AbstractConcurrentMapFilterCache;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A soft reference based filter cache that has soft keys on the <tt>IndexReader</tt>.
 *
 * @author kimchy (shay.banon)
 */
public class SoftFilterCache extends AbstractConcurrentMapFilterCache implements MapEvictionListener<Filter, DocSet> {

    private final int maxSize;

    private final TimeValue expire;

    private final AtomicLong evictions = new AtomicLong();

    @Inject public SoftFilterCache(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
        this.maxSize = componentSettings.getAsInt("max_size", -1);
        this.expire = componentSettings.getAsTime("expire", null);
    }

    @Override protected ConcurrentMap<Filter, DocSet> buildFilterMap() {
        // DocSet are not really stored with strong reference only when searching on them...
        // Filter might be stored in query cache
        MapMaker mapMaker = new MapMaker().softValues();
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
        return "soft";
    }

    @Override public long evictions() {
        return evictions.get();
    }

    @Override public void onEviction(Filter filter, DocSet docSet) {
        evictions.incrementAndGet();
    }
}
