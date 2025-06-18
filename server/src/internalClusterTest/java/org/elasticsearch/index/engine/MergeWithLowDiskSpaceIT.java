/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.DiskUsageIntegTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Locale;
import java.util.stream.IntStream;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class MergeWithLowDiskSpaceIT extends DiskUsageIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // the default of "5s" slows down testing
            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true)
            .build();
    }

    public void testShardCloseWhenDiskSpaceInsufficient() {
        internalCluster().startNode();
        ensureStableCluster(1);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build()
        );
        indexRandom(
            true,
            IntStream.range(1, 100)
                .mapToObj(i -> prepareIndex(indexName).setSource("field", randomAlphaOfLength(50)))
                .toArray(IndexRequestBuilder[]::new)
        );
    }
}
