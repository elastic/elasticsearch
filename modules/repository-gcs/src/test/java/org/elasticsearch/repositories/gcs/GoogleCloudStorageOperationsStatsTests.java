/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import org.elasticsearch.common.blobstore.BlobStoreActionStats;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.repositories.gcs.GoogleCloudStorageOperationsStats.Operation;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;

import static org.elasticsearch.common.blobstore.OperationPurpose.CLUSTER_STATE;
import static org.elasticsearch.common.blobstore.OperationPurpose.SNAPSHOT_DATA;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageOperationsStats.Operation.GET_OBJECT;
import static org.elasticsearch.repositories.gcs.GoogleCloudStorageOperationsStats.Operation.LIST_OBJECTS;

public class GoogleCloudStorageOperationsStatsTests extends ESTestCase {

    public void testToMap() {
        var stats = new GoogleCloudStorageOperationsStats("bucketName");
        stats.trackOperation(SNAPSHOT_DATA, GET_OBJECT);
        stats.trackRequest(SNAPSHOT_DATA, GET_OBJECT);
        stats.trackOperation(SNAPSHOT_DATA, LIST_OBJECTS);
        stats.trackRequest(CLUSTER_STATE, LIST_OBJECTS);

        assertEquals(
            new StatsMap().add(SNAPSHOT_DATA, GET_OBJECT, 1, 1)
                .add(SNAPSHOT_DATA, LIST_OBJECTS, 1, 0)
                .add(CLUSTER_STATE, LIST_OBJECTS, 0, 1),
            stats.toMap()
        );
    }

    static class StatsMap extends HashMap<String, BlobStoreActionStats> {
        StatsMap() {
            for (var purpose : OperationPurpose.values()) {
                for (var operation : Operation.values()) {
                    put(purpose + "_" + operation, new BlobStoreActionStats(0, 0));
                }
            }
        }

        StatsMap add(OperationPurpose purpose, Operation operation, long ops, long reqs) {
            put(purpose + "_" + operation, new BlobStoreActionStats(ops, reqs));
            return this;
        }
    }

}
