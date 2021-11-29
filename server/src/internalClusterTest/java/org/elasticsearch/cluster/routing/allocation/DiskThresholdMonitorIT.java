/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.DiskUsageIntegTestCase;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Locale;

import static org.elasticsearch.index.store.Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DiskThresholdMonitorIT extends DiskUsageIntegTestCase {

    private static final long FLOODSTAGE_BYTES = new ByteSizeValue(10, ByteSizeUnit.KB).getBytes();

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), FLOODSTAGE_BYTES * 2 + "b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), FLOODSTAGE_BYTES * 2 + "b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), FLOODSTAGE_BYTES + "b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "0ms")
            .build();
    }

    public void testFloodStageExceeded() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();

        final InternalClusterInfoService clusterInfoService = (InternalClusterInfoService) internalCluster().getCurrentMasterNodeInstance(
            ClusterInfoService.class
        );

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), "0ms")
                .build()
        );
        // ensure we have a system index on the data node too.
        assertAcked(client().admin().indices().prepareCreate(TaskResultsService.TASK_INDEX));

        getTestFileStore(dataNodeName).setTotalSpace(1L);
        refreshClusterInfo();
        assertBusy(() -> {
            assertBlocked(
                client().prepareIndex().setIndex(indexName).setId("1").setSource("f", "g"),
                IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK
            );
            assertThat(
                client().admin()
                    .indices()
                    .prepareGetSettings(indexName)
                    .setNames(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE)
                    .get()
                    .getSetting(indexName, IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE),
                equalTo("true")
            );
        });
    }
}
