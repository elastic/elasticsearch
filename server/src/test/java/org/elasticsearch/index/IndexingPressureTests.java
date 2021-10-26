/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.stats.IndexingPressureStats;
import org.elasticsearch.test.ESTestCase;

public class IndexingPressureTests extends ESTestCase {

    private final Settings settings = Settings.builder().put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "10KB").build();

    public void testMemoryBytesAndOpsMarkedAndReleased() {
        IndexingPressure indexingPressure = new IndexingPressure(settings);
        try (Releasable coordinating = indexingPressure.markCoordinatingOperationStarted(1, 10, false);
             Releasable coordinating2 = indexingPressure.markCoordinatingOperationStarted(5, 50, false);
             Releasable primary = indexingPressure.markPrimaryOperationStarted(2, 15, true);
             Releasable primary2 = indexingPressure.markPrimaryOperationStarted(1, 5, false);
             Releasable replica = indexingPressure.markReplicaOperationStarted(3, 25, true);
             Releasable replica2 = indexingPressure.markReplicaOperationStarted(1, 10, false)) {
            IndexingPressureStats stats = indexingPressure.stats();
            assertEquals(60, stats.getCurrentCoordinatingBytes());
            assertEquals(20, stats.getCurrentPrimaryBytes());
            assertEquals(80, stats.getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(35, stats.getCurrentReplicaBytes());

            assertEquals(6, stats.getTotalCoordinatingOps());
            assertEquals(3, stats.getTotalPrimaryOps());
            assertEquals(4, stats.getTotalReplicaOps());
        }
        IndexingPressureStats stats = indexingPressure.stats();
        assertEquals(0, stats.getCurrentCoordinatingBytes());
        assertEquals(0, stats.getCurrentPrimaryBytes());
        assertEquals(0, stats.getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(0, stats.getCurrentReplicaBytes());

        assertEquals(0, stats.getCurrentCoordinatingOps());
        assertEquals(0, stats.getCurrentPrimaryOps());
        assertEquals(0, stats.getCurrentReplicaOps());

        assertEquals(60, stats.getTotalCoordinatingBytes());
        assertEquals(20, stats.getTotalPrimaryBytes());
        assertEquals(80, stats.getTotalCombinedCoordinatingAndPrimaryBytes());
        assertEquals(35, stats.getTotalReplicaBytes());

        assertEquals(6, stats.getTotalCoordinatingOps());
        assertEquals(3, stats.getTotalPrimaryOps());
        assertEquals(4, stats.getTotalReplicaOps());
    }

    public void testAvoidDoubleMemoryAccounting() {
        IndexingPressure indexingPressure = new IndexingPressure(settings);
        try (Releasable coordinating = indexingPressure.markCoordinatingOperationStarted(1, 10, false);
             Releasable primary = indexingPressure.markPrimaryOperationLocalToCoordinatingNodeStarted(1, 15)) {
            IndexingPressureStats stats = indexingPressure.stats();
            assertEquals(10, stats.getCurrentCoordinatingBytes());
            assertEquals(15, stats.getCurrentPrimaryBytes());
            assertEquals(10, stats.getCurrentCombinedCoordinatingAndPrimaryBytes());
        }
        IndexingPressureStats stats = indexingPressure.stats();
        assertEquals(0, stats.getCurrentCoordinatingBytes());
        assertEquals(0, stats.getCurrentPrimaryBytes());
        assertEquals(0, stats.getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(10, stats.getTotalCoordinatingBytes());
        assertEquals(15, stats.getTotalPrimaryBytes());
        assertEquals(10, stats.getTotalCombinedCoordinatingAndPrimaryBytes());
    }

    public void testCoordinatingPrimaryRejections() {
        IndexingPressure indexingPressure = new IndexingPressure(settings);
        try (Releasable coordinating = indexingPressure.markCoordinatingOperationStarted(1, 1024 * 3, false);
             Releasable primary = indexingPressure.markPrimaryOperationStarted(1, 1024 * 3, false);
             Releasable replica = indexingPressure.markReplicaOperationStarted(1, 1024 * 3, false)) {
            if (randomBoolean()) {
                expectThrows(EsRejectedExecutionException.class,
                    () -> indexingPressure.markCoordinatingOperationStarted(1, 1024 * 2, false));
                IndexingPressureStats stats = indexingPressure.stats();
                assertEquals(1, stats.getCoordinatingRejections());
                assertEquals(1024 * 6, stats.getCurrentCombinedCoordinatingAndPrimaryBytes());
            } else {
                expectThrows(EsRejectedExecutionException.class, () -> indexingPressure.markPrimaryOperationStarted(1, 1024 * 2, false));
                IndexingPressureStats stats = indexingPressure.stats();
                assertEquals(1, stats.getPrimaryRejections());
                assertEquals(1024 * 6, stats.getCurrentCombinedCoordinatingAndPrimaryBytes());
            }
            long preForceRejections = indexingPressure.stats().getPrimaryRejections();
            // Primary can be forced
            Releasable forced = indexingPressure.markPrimaryOperationStarted(1, 1024 * 2, true);
            assertEquals(preForceRejections, indexingPressure.stats().getPrimaryRejections());
            assertEquals(1024 * 8, indexingPressure.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
            forced.close();

            // Local to coordinating node primary actions not rejected
            IndexingPressureStats preLocalStats = indexingPressure.stats();
            Releasable local = indexingPressure.markPrimaryOperationLocalToCoordinatingNodeStarted(1, 1024 * 2);
            assertEquals(preLocalStats.getPrimaryRejections(), indexingPressure.stats().getPrimaryRejections());
            assertEquals(1024 * 6, indexingPressure.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(preLocalStats.getCurrentPrimaryBytes() + 1024 * 2, indexingPressure.stats().getCurrentPrimaryBytes());
            local.close();
        }

        assertEquals(1024 * 8, indexingPressure.stats().getTotalCombinedCoordinatingAndPrimaryBytes());
    }

    public void testReplicaRejections() {
        IndexingPressure indexingPressure = new IndexingPressure(settings);
        try (Releasable coordinating = indexingPressure.markCoordinatingOperationStarted(1, 1024 * 3, false);
             Releasable primary = indexingPressure.markPrimaryOperationStarted(1, 1024 * 3, false);
             Releasable replica = indexingPressure.markReplicaOperationStarted(1, 1024 * 3, false)) {
            // Replica will not be rejected until replica bytes > 15KB
            Releasable replica2 = indexingPressure.markReplicaOperationStarted(1, 1024 * 11, false);
            assertEquals(1024 * 14, indexingPressure.stats().getCurrentReplicaBytes());
            // Replica will be rejected once we cross 15KB
            expectThrows(EsRejectedExecutionException.class, () -> indexingPressure.markReplicaOperationStarted(1, 1024 * 2, false));
            IndexingPressureStats stats = indexingPressure.stats();
            assertEquals(1, stats.getReplicaRejections());
            assertEquals(1024 * 14, stats.getCurrentReplicaBytes());

            // Replica can be forced
            Releasable forced = indexingPressure.markPrimaryOperationStarted(1, 1024 * 2, true);
            assertEquals(1, indexingPressure.stats().getReplicaRejections());
            assertEquals(1024 * 14, indexingPressure.stats().getCurrentReplicaBytes());
            forced.close();
            replica2.close();
        }

        assertEquals(1024 * 14, indexingPressure.stats().getTotalReplicaBytes());
    }

    public void testForceExecutionOnCoordinating() {
        IndexingPressure indexingPressure = new IndexingPressure(settings);
        expectThrows(EsRejectedExecutionException.class, () -> indexingPressure.markCoordinatingOperationStarted(1, 1024 * 11, false));
        try (Releasable ignore = indexingPressure.markCoordinatingOperationStarted(1, 1024 * 11, true)) {
            assertEquals(1024 * 11, indexingPressure.stats().getCurrentCoordinatingBytes());
        }
        assertEquals(0, indexingPressure.stats().getCurrentCoordinatingBytes());
    }
}
