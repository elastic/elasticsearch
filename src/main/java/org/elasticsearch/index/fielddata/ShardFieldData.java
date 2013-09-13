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

package org.elasticsearch.index.fielddata;

import com.carrotsearch.hppc.ObjectLongOpenHashMap;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 */
public class ShardFieldData extends AbstractIndexShardComponent implements IndexFieldDataCache.Listener {

    final CounterMetric evictionsMetric = new CounterMetric();
    final CounterMetric totalMetric = new CounterMetric();

    final ConcurrentMap<String, CounterMetric> perFieldTotals = ConcurrentCollections.newConcurrentMap();

    @Inject
    public ShardFieldData(ShardId shardId, @IndexSettings Settings indexSettings) {
        super(shardId, indexSettings);
    }

    public FieldDataStats stats(String... fields) {
        ObjectLongOpenHashMap<String> fieldTotals = null;
        if (fields != null && fields.length > 0) {
            fieldTotals = new ObjectLongOpenHashMap<String>();
            for (Map.Entry<String, CounterMetric> entry : perFieldTotals.entrySet()) {
                for (String field : fields) {
                    if (Regex.simpleMatch(field, entry.getKey())) {
                        fieldTotals.put(entry.getKey(), entry.getValue().count());
                    }
                }
            }
        }
        return new FieldDataStats(totalMetric.count(), evictionsMetric.count(), fieldTotals);
    }

    @Override
    public void onLoad(FieldMapper.Names fieldNames, FieldDataType fieldDataType, AtomicFieldData fieldData) {
        long sizeInBytes = fieldData.getMemorySizeInBytes();

        totalMetric.inc(sizeInBytes);

        String keyFieldName = fieldNames.indexName();
        CounterMetric total = perFieldTotals.get(keyFieldName);
        if (total != null) {
            total.inc(sizeInBytes);
        } else {
            total = new CounterMetric();
            total.inc(sizeInBytes);
            CounterMetric prev = perFieldTotals.putIfAbsent(keyFieldName, total);
            if (prev != null) {
                prev.inc(sizeInBytes);
            }
        }
    }

    @Override
    public void onUnload(FieldMapper.Names fieldNames, FieldDataType fieldDataType, boolean wasEvicted, long sizeInBytes, @Nullable AtomicFieldData fieldData) {
        if (wasEvicted) {
            evictionsMetric.inc();
        }
        if (sizeInBytes != -1) {
            totalMetric.dec(sizeInBytes);

            String keyFieldName = fieldNames.indexName();
            CounterMetric total = perFieldTotals.get(keyFieldName);
            if (total != null) {
                total.dec(sizeInBytes);
            }
        }
    }
}
