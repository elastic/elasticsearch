/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.searchablesnapshots;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotStats.CacheDirectoryStats;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotStats.CacheIndexInputStats;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotStats.Counter;

import java.util.ArrayList;
import java.util.List;

public class SearchableSnapshotStatsTests extends AbstractWireSerializingTestCase<SearchableSnapshotStats> {

    @Override
    protected Writeable.Reader<SearchableSnapshotStats> instanceReader() {
        return SearchableSnapshotStats::new;
    }

    @Override
    protected SearchableSnapshotStats createTestInstance() {
        final List<CacheDirectoryStats> directoryStats = new ArrayList<>();
        for (int i = 0; i < randomInt(20); i++) {
            directoryStats.add(randomCacheDirectoryStats());
        }
        return new SearchableSnapshotStats(directoryStats);
    }

    private CacheDirectoryStats randomCacheDirectoryStats() {
        SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(5), randomAlphaOfLength(5));
        IndexId indexId = new IndexId(randomAlphaOfLength(5), randomAlphaOfLength(5));
        ShardId shardId = new ShardId(randomAlphaOfLength(5), randomAlphaOfLength(5), randomInt(10));

        final List<CacheIndexInputStats> inputStats = new ArrayList<>();
        for (int j = 0; j < randomInt(20); j++) {
            inputStats.add(randomCacheIndexInputStats());
        }
        return new CacheDirectoryStats(snapshotId, indexId, shardId, inputStats);
    }

    private CacheIndexInputStats randomCacheIndexInputStats() {
        return new CacheIndexInputStats(randomAlphaOfLength(10), randomNonNegativeLong(),
            randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
            randomCounter(), randomCounter(),
            randomCounter(), randomCounter(),
            randomCounter(), randomCounter(),
            randomCounter(), randomCounter(),
            randomCounter());
    }

    private Counter randomCounter() {
        return new Counter(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }
}
