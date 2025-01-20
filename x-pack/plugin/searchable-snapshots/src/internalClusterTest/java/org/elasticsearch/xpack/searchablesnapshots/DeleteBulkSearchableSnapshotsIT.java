/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;

import static org.elasticsearch.cluster.coordination.LagDetector.CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING;

@ESIntegTestCase.ClusterScope(supportsDedicatedMasters = true, numClientNodes = 0, numDataNodes = 0)
public class DeleteBulkSearchableSnapshotsIT extends BaseSearchableSnapshotsIntegTestCase {
    private static final int NUMBER_OF_INDICES = Integer.parseInt(System.getProperty("testDeletions.numberOfIndices", "150"));

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMillis(3000))
            .build();
    }

    public void testDeleteManySearchableSnapshotIndices() throws Exception {
        internalCluster().startMasterOnlyNodes(3);
        internalCluster().startDataOnlyNodes(2);

        String indexName = randomIdentifier();
        createIndex(indexName);
        indexRandom(true, indexName, 20);

        String repositoryName = randomIdentifier();
        createRepository(repositoryName, "fs");

        String snapshotName = randomIdentifier();
        SnapshotInfo snapshot = createSnapshot(repositoryName, snapshotName, List.of(indexName));

        int numberOfIndices = NUMBER_OF_INDICES;
        logger.info("Mounting {} searchable snapshots", numberOfIndices);
        final String indexPrefix = randomIdentifier() + "_";
        final String otherIndexPrefix = randomIdentifier() + "_";
        for (int i = 0; i < numberOfIndices; i++) {
            String searchableSnapshotName = i < (0.8 * numberOfIndices) ? indexPrefix + i : otherIndexPrefix + i;
            mountSnapshot(repositoryName, snapshotName, indexName, searchableSnapshotName, Settings.EMPTY);
            ensureGreen(searchableSnapshotName);
        }

        long startTime = System.currentTimeMillis();
        client().execute(TransportDeleteIndexAction.TYPE, new DeleteIndexRequest(indexPrefix + "*")).actionGet();
        logger.info("Deleting {} indices took {}", numberOfIndices, TimeValue.timeValueMillis(System.currentTimeMillis() - startTime));

        createIndex("bump_cluster_state_1");
        safeSleep(1_000);

        createIndex("bump_cluster_state_2");
        safeSleep(1_000);

        createIndex("bump_cluster_state_3");
        safeSleep(1_000);

        safeSleep(30_000);
        logger.info("Continuing");
    }

    @Override
    protected int maximumNumberOfShards() {
        return 2;
    }

    @Override
    protected int minimumNumberOfShards() {
        return 2;
    }
}
