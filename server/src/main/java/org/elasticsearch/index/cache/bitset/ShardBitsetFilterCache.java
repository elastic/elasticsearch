/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.cache.bitset;

import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

public class ShardBitsetFilterCache extends AbstractIndexShardComponent {

    private final CounterMetric totalMetric = new CounterMetric();

    public ShardBitsetFilterCache(ShardId shardId, IndexSettings indexSettings) {
        super(shardId, indexSettings);
    }

    public void onCached(long sizeInBytes) {
        totalMetric.inc(sizeInBytes);
    }

    public void onRemoval(long sizeInBytes) {
        totalMetric.dec(sizeInBytes);
    }

    public long getMemorySizeInBytes() {
        return totalMetric.count();
    }

}
