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

package org.elasticsearch.index.cache.field.data.weak;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.MapEvictionListener;
import org.elasticsearch.common.collect.MapMaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.field.data.support.AbstractConcurrentMapFieldDataCache;
import org.elasticsearch.index.field.data.FieldData;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.concurrent.ConcurrentMap;

/**
 * @author kimchy (shay.banon)
 */
public class WeakFieldDataCache extends AbstractConcurrentMapFieldDataCache implements MapEvictionListener<String, FieldData> {

    private final CounterMetric evictions = new CounterMetric();

    @Inject public WeakFieldDataCache(Index index, @IndexSettings Settings indexSettings) {
        super(index, indexSettings);
    }

    @Override protected ConcurrentMap<String, FieldData> buildFieldDataMap() {
        return new MapMaker().weakValues().evictionListener(this).makeMap();
    }

    @Override public String type() {
        return "weak";
    }

    @Override public long evictions() {
        return evictions.count();
    }

    @Override public void onEviction(@Nullable String s, @Nullable FieldData fieldData) {
        evictions.inc();
    }
}