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

import java.util.List;

public class DeleteBulkSearchableSnapshotsIT extends BaseSearchableSnapshotsIntegTestCase {
    private static final int NUMBER_OF_INDICES = Integer.parseInt(System.getProperty("testDeletions.numberOfIndices", "50"));

    public void testDeleteManySearchableSnapshotIndices() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(3);

        String indexName = randomIdentifier();
        createIndex(indexName);
        indexRandom(true, indexName, randomIntBetween(10_000, 100_000));

        String repositoryName = randomIdentifier();
        createRepository(repositoryName, "fs");

        String snapshotName = randomIdentifier();
        SnapshotInfo snapshot = createSnapshot(repositoryName, snapshotName, List.of(indexName));

        int numberOfIndices = NUMBER_OF_INDICES;
        logger.info("Mounting {} searchable snapshots", numberOfIndices);
        final String indexPrefix = randomIdentifier() + "_";
        for (int i = 0; i < numberOfIndices; i++) {
            String searchableSnapshotName = indexPrefix + i;
            mountSnapshot(repositoryName, snapshotName, indexName, searchableSnapshotName, Settings.EMPTY);
            ensureGreen(searchableSnapshotName);
        }

        long startTime = System.currentTimeMillis();
        client().execute(TransportDeleteIndexAction.TYPE, new DeleteIndexRequest("_all")).actionGet();
        logger.info("Deleting {} indices took {}", numberOfIndices, TimeValue.timeValueMillis(System.currentTimeMillis() - startTime));
    }
}
