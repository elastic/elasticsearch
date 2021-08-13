/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.searchablesnapshots;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats.CacheIndexInputStats;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats.Counter;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats.TimedCounter;

import java.util.ArrayList;
import java.util.List;

public class SearchableSnapshotShardStatsTests extends AbstractWireSerializingTestCase<SearchableSnapshotShardStats> {

    @Override
    protected Writeable.Reader<SearchableSnapshotShardStats> instanceReader() {
        return SearchableSnapshotShardStats::new;
    }

    @Override
    protected SearchableSnapshotShardStats createTestInstance() {
        SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(5), randomAlphaOfLength(5));
        IndexId indexId = new IndexId(randomAlphaOfLength(5), randomAlphaOfLength(5));
        ShardRouting shardRouting = TestShardRouting.newShardRouting(randomAlphaOfLength(5), randomInt(10), randomAlphaOfLength(5),
            randomBoolean(), ShardRoutingState.STARTED);

        final List<CacheIndexInputStats> inputStats = new ArrayList<>();
        for (int j = 0; j < randomInt(20); j++) {
            inputStats.add(randomCacheIndexInputStats());
        }
        return new SearchableSnapshotShardStats(shardRouting, snapshotId, indexId, inputStats);
    }

    private CacheIndexInputStats randomCacheIndexInputStats() {
        return new CacheIndexInputStats(randomAlphaOfLength(10), randomNonNegativeLong(), new ByteSizeValue(randomNonNegativeLong()),
            new ByteSizeValue(randomNonNegativeLong()), new ByteSizeValue(randomNonNegativeLong()),
            randomNonNegativeLong(), randomNonNegativeLong(),
            randomCounter(), randomCounter(),
            randomCounter(), randomCounter(),
            randomCounter(), randomCounter(),
            randomCounter(), randomCounter(), randomTimedCounter(),
            randomTimedCounter(), randomTimedCounter(),
            randomCounter(), randomCounter(), randomNonNegativeLong());
    }

    private Counter randomCounter() {
        return new Counter(randomLong(), randomLong(), randomLong(), randomLong());
    }

    private TimedCounter randomTimedCounter() {
        return new TimedCounter(randomLong(), randomLong(), randomLong(), randomLong(), randomLong());
    }
}
