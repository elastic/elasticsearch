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

package org.elasticsearch.index.cache.field.data.resident;

import com.google.common.base.Objects;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.field.data.support.AbstractConcurrentMapFieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ResidentFieldDataCache extends AbstractConcurrentMapFieldDataCache implements RemovalListener<String, FieldData> {

    private final IndexSettingsService indexSettingsService;

    private volatile int maxSize;
    private volatile TimeValue expire;

    private final CounterMetric evictions = new CounterMetric();

    private final ApplySettings applySettings = new ApplySettings();

    @Inject
    public ResidentFieldDataCache(Index index, @IndexSettings Settings indexSettings, IndexSettingsService indexSettingsService) {
        super(index, indexSettings);
        this.indexSettingsService = indexSettingsService;

        this.maxSize = indexSettings.getAsInt(INDEX_CACHE_FIELD_MAX_SIZE, componentSettings.getAsInt("max_size", -1));
        this.expire = indexSettings.getAsTime(INDEX_CACHE_FIELD_EXPIRE, componentSettings.getAsTime("expire", null));
        logger.debug("using [resident] field cache with max_size [{}], expire [{}]", maxSize, expire);

        indexSettingsService.addListener(applySettings);
    }

    @Override
    public void close() throws ElasticSearchException {
        indexSettingsService.removeListener(applySettings);
        super.close();
    }

    @Override
    protected Cache<String, FieldData> buildFieldDataMap() {
        CacheBuilder<String, FieldData> cacheBuilder = CacheBuilder.newBuilder().removalListener(this);
        if (maxSize != -1) {
            cacheBuilder.maximumSize(maxSize);
        }
        if (expire != null) {
            cacheBuilder.expireAfterAccess(expire.nanos(), TimeUnit.NANOSECONDS);
        }
        return cacheBuilder.build();
    }

    @Override
    public String type() {
        return "resident";
    }

    @Override
    public long evictions() {
        return evictions.count();
    }

    @Override
    public void onRemoval(RemovalNotification<String, FieldData> removalNotification) {
        if (removalNotification.wasEvicted()) {
            evictions.inc();
        }
    }

    public static final String INDEX_CACHE_FIELD_MAX_SIZE = "index.cache.field.max_size";
    public static final String INDEX_CACHE_FIELD_EXPIRE = "index.cache.field.expire";

    class ApplySettings implements IndexSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            int maxSize = settings.getAsInt(INDEX_CACHE_FIELD_MAX_SIZE, ResidentFieldDataCache.this.maxSize);
            TimeValue expire = settings.getAsTime(INDEX_CACHE_FIELD_EXPIRE, ResidentFieldDataCache.this.expire);
            boolean changed = false;
            if (maxSize != ResidentFieldDataCache.this.maxSize) {
                logger.info("updating index.cache.field.max_size from [{}] to [{}]", ResidentFieldDataCache.this.maxSize, maxSize);
                changed = true;
                ResidentFieldDataCache.this.maxSize = maxSize;
            }
            if (!Objects.equal(expire, ResidentFieldDataCache.this.expire)) {
                logger.info("updating index.cache.field.expire from [{}] to [{}]", ResidentFieldDataCache.this.expire, expire);
                changed = true;
                ResidentFieldDataCache.this.expire = expire;
            }
            if (changed) {
                clear("update_settings");
            }
        }
    }
}
