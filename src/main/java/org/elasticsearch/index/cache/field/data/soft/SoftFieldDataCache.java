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

package org.elasticsearch.index.cache.field.data.soft;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.elasticsearch.common.cache.CacheBuilderHelper;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.field.data.support.AbstractConcurrentMapFieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.settings.IndexSettings;

/**
 *
 */
public class SoftFieldDataCache extends AbstractConcurrentMapFieldDataCache implements RemovalListener<String, FieldData> {

    private final CounterMetric evictions = new CounterMetric();

    @Inject
    public SoftFieldDataCache(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
    }

    @Override
    protected Cache<String, FieldData> buildFieldDataMap() {
        CacheBuilder<String, FieldData> cacheBuilder = CacheBuilder.newBuilder().softValues().removalListener(this);
        CacheBuilderHelper.disableStats(cacheBuilder);
        return cacheBuilder.build();
    }

    @Override
    public long evictions() {
        return evictions.count();
    }

    @Override
    public String type() {
        return "soft";
    }

    @Override
    public void onRemoval(RemovalNotification<String, FieldData> removalNotification) {
        if (removalNotification.wasEvicted()) {
            evictions.inc();
        }
    }
}
