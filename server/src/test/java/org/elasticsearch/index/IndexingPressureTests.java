/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.stats.IndexingPressureStats;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.Optional;

public class IndexingPressureTests extends ESTestCase {

    private final Settings settings = Settings.builder()
        .put(IndexingPressure.MAX_COORDINATING_BYTES.getKey(), "10KB")
        .put(IndexingPressure.MAX_PRIMARY_BYTES.getKey(), "12KB")
        .put(IndexingPressure.MAX_REPLICA_BYTES.getKey(), "15KB")
        .put(IndexingPressure.SPLIT_BULK_LOW_WATERMARK.getKey(), "8KB")
        .put(IndexingPressure.SPLIT_BULK_LOW_WATERMARK_SIZE.getKey(), "1KB")
        .put(IndexingPressure.SPLIT_BULK_HIGH_WATERMARK.getKey(), "9KB")
        .put(IndexingPressure.SPLIT_BULK_HIGH_WATERMARK_SIZE.getKey(), "128B")
        .put(IndexingPressure.MAX_OPERATION_SIZE.getKey(), "128B")
        .build();

    public void testMemoryLimitSettingsFallbackToOldSingleLimitSetting() {
        Settings settings = Settings.builder().put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "20KB").build();

        assertThat(IndexingPressure.MAX_COORDINATING_BYTES.get(settings), Matchers.equalTo(ByteSizeValue.ofKb(20)));
        assertThat(IndexingPressure.MAX_PRIMARY_BYTES.get(settings), Matchers.equalTo(ByteSizeValue.ofKb(20)));
        assertThat(IndexingPressure.MAX_REPLICA_BYTES.get(settings), Matchers.equalTo(ByteSizeValue.ofKb(30)));
    }

    public void testHighAndLowWatermarkSplits() {
        IndexingPressure indexingPressure = new IndexingPressure(settings);

        try (
            IndexingPressure.Incremental coordinating1 = indexingPressure.startIncrementalCoordinating(10, randomIntBetween(1, 127), false);
            IndexingPressure.Incremental coordinating2 = indexingPressure.startIncrementalCoordinating(
                10,
                randomIntBetween(128, 1023),
                false
            );
            IndexingPressure.Incremental coordinating3 = indexingPressure.startIncrementalCoordinating(
                10,
                randomIntBetween(1024, 6000),
                false
            );
            Releasable ignored1 = indexingPressure.startIncrementalCoordinating(
                10,
                1 + (8 * 1024) - indexingPressure.stats().getCurrentCoordinatingBytes(),
                false
            )
        ) {
            assertFalse(coordinating1.maybeSplit().isPresent());
            assertFalse(coordinating2.maybeSplit().isPresent());
            assertEquals(indexingPressure.stats().getHighWaterMarkSplits(), 0L);
            assertEquals(indexingPressure.stats().getLowWaterMarkSplits(), 0L);
            Optional<Releasable> split1 = coordinating3.maybeSplit();
            assertTrue(split1.isPresent());
            try (Releasable ignored2 = split1.get()) {
                assertEquals(indexingPressure.stats().getHighWaterMarkSplits(), 0L);
                assertEquals(indexingPressure.stats().getLowWaterMarkSplits(), 1L);

                try (
                    Releasable ignored3 = indexingPressure.markCoordinatingOperationStarted(
                        10,
                        1 + (9 * 1024) - indexingPressure.stats().getCurrentCoordinatingBytes(),
                        false
                    )
                ) {
                    assertFalse(coordinating1.maybeSplit().isPresent());
                    Optional<Releasable> split2 = coordinating2.maybeSplit();
                    assertTrue(split2.isPresent());
                    try (Releasable ignored4 = split2.get()) {
                        assertEquals(indexingPressure.stats().getHighWaterMarkSplits(), 1L);
                        assertEquals(indexingPressure.stats().getLowWaterMarkSplits(), 1L);
                    }
                }
            }
        }
    }

    public void testMemoryBytesAndOpsMarkedAndReleased() {
        IndexingPressure indexingPressure = new IndexingPressure(settings);
        try (
            Releasable coordinating = indexingPressure.markCoordinatingOperationStarted(1, 10, false);
            Releasable coordinating2 = indexingPressure.markCoordinatingOperationStarted(5, 50, false);
            Releasable primary = indexingPressure.markPrimaryOperationStarted(2, 15, true);
            Releasable primary2 = indexingPressure.markPrimaryOperationStarted(1, 5, false);
            Releasable replica = indexingPressure.markReplicaOperationStarted(3, 25, true);
            Releasable replica2 = indexingPressure.markReplicaOperationStarted(1, 10, false)
        ) {
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
        try (
            Releasable coordinating = indexingPressure.markCoordinatingOperationStarted(1, 10, false);
            Releasable primary = indexingPressure.validateAndMarkPrimaryOperationLocalToCoordinatingNodeStarted(1, 15, 15, true)
        ) {
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
        try (
            Releasable coordinating = indexingPressure.markCoordinatingOperationStarted(1, 1024 * 3, false);
            Releasable primary = indexingPressure.markPrimaryOperationStarted(1, 1024 * 3, false);
            Releasable replica = indexingPressure.markReplicaOperationStarted(1, 1024 * 3, false)
        ) {
            if (randomBoolean()) {
                expectThrows(
                    EsRejectedExecutionException.class,
                    () -> indexingPressure.markCoordinatingOperationStarted(1, 1024 * 2, false)
                );
                IndexingPressureStats stats = indexingPressure.stats();
                assertEquals(1, stats.getCoordinatingRejections());
                assertEquals(1024 * 6, stats.getCurrentCombinedCoordinatingAndPrimaryBytes());
            } else {
                expectThrows(EsRejectedExecutionException.class, () -> indexingPressure.markPrimaryOperationStarted(1, 1024 * 4, false));
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
            Releasable local = indexingPressure.validateAndMarkPrimaryOperationLocalToCoordinatingNodeStarted(1, 1024 * 2, 1024 * 2, true);
            assertEquals(preLocalStats.getPrimaryRejections(), indexingPressure.stats().getPrimaryRejections());
            assertEquals(1024 * 6, indexingPressure.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(preLocalStats.getCurrentPrimaryBytes() + 1024 * 2, indexingPressure.stats().getCurrentPrimaryBytes());
            local.close();
        }

        assertEquals(1024 * 8, indexingPressure.stats().getTotalCombinedCoordinatingAndPrimaryBytes());
    }

    public void testReplicaRejections() {
        IndexingPressure indexingPressure = new IndexingPressure(settings);
        try (
            Releasable coordinating = indexingPressure.markCoordinatingOperationStarted(1, 1024 * 3, false);
            Releasable primary = indexingPressure.markPrimaryOperationStarted(1, 1024 * 3, false);
            Releasable replica = indexingPressure.markReplicaOperationStarted(1, 1024 * 3, false)
        ) {
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

    public void testPrimaryOperationExpansionAccounting() {
        IndexingPressure indexingPressure = new IndexingPressure(settings);
        // Primary limit is 12kb
        try (
            Releasable coordinating = indexingPressure.markCoordinatingOperationStarted(2, 1024 * 3, false);
            Releasable primary = indexingPressure.markPrimaryOperationStarted(2, 1024 * 3, false);
        ) {
            var opsExpansionReleasable = indexingPressure.trackPrimaryOperationExpansion(2, 1024 * 3, false);
            assertEquals(0, indexingPressure.stats().getPrimaryRejections());
            assertEquals(0, indexingPressure.stats().getPrimaryDocumentRejections());
            assertEquals(1024 * 9, indexingPressure.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());

            opsExpansionReleasable.close();
        }

        assertEquals(1024 * 6, indexingPressure.stats().getTotalPrimaryBytes());
        // ensure that the expansion does not increase the number of ops twice
        assertEquals(2, indexingPressure.stats().getTotalPrimaryOps());
    }

    public void testPrimaryOperationExpansionAccountingRejections() {
        IndexingPressure indexingPressure = new IndexingPressure(settings);
        // Primary limit is 12kb
        try (
            Releasable coordinating = indexingPressure.markCoordinatingOperationStarted(2, 1024 * 3, false);
            Releasable primary = indexingPressure.markPrimaryOperationStarted(2, 1024 * 3, false);
        ) {
            expectThrows(EsRejectedExecutionException.class, () -> indexingPressure.trackPrimaryOperationExpansion(2, 1024 * 8, false));
            assertEquals(1, indexingPressure.stats().getPrimaryRejections());
            assertEquals(2, indexingPressure.stats().getPrimaryDocumentRejections());
            assertEquals(1024 * 6, indexingPressure.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());

            var forcedOpsExpansionReleasable = indexingPressure.trackPrimaryOperationExpansion(2, 1024 * 8, true);
            assertEquals(1, indexingPressure.stats().getPrimaryRejections());
            assertEquals(2, indexingPressure.stats().getPrimaryDocumentRejections());
            assertEquals(1024 * 14, indexingPressure.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());

            forcedOpsExpansionReleasable.close();
        }

        assertEquals(1024 * 11, indexingPressure.stats().getTotalPrimaryBytes());
        assertEquals(2, indexingPressure.stats().getTotalPrimaryOps());
    }

    public void testReplicaOperationExpansionAccounting() {
        IndexingPressure indexingPressure = new IndexingPressure(settings);
        // Replica limit is 15kb
        try (
            Releasable coordinating = indexingPressure.markCoordinatingOperationStarted(2, 1024 * 3, false);
            Releasable primary = indexingPressure.markPrimaryOperationStarted(2, 1024 * 3, false);
            Releasable replica = indexingPressure.markReplicaOperationStarted(2, 1024 * 3, false);
        ) {
            var opsExpansionReleasable = indexingPressure.trackReplicaOperationExpansion(1024 * 3, false);
            assertEquals(0, indexingPressure.stats().getReplicaRejections());
            assertEquals(1024 * 6, indexingPressure.stats().getCurrentReplicaBytes());

            opsExpansionReleasable.close();
        }

        assertEquals(1024 * 6, indexingPressure.stats().getTotalReplicaBytes());
        // ensure that the expansion does not increase the number of ops twice
        assertEquals(2, indexingPressure.stats().getTotalReplicaOps());
    }

    public void testReplicaOperationRejectionsExpansionAccounting() {
        IndexingPressure indexingPressure = new IndexingPressure(settings);
        // Replica limit is 15kb
        try (
            Releasable coordinating = indexingPressure.markCoordinatingOperationStarted(2, 1024 * 3, false);
            Releasable primary = indexingPressure.markPrimaryOperationStarted(2, 1024 * 3, false);
            Releasable replica = indexingPressure.markReplicaOperationStarted(2, 1024 * 3, false);
        ) {
            expectThrows(EsRejectedExecutionException.class, () -> indexingPressure.trackReplicaOperationExpansion(1024 * 16, false));
            assertEquals(1, indexingPressure.stats().getReplicaRejections());
            assertEquals(1024 * 3, indexingPressure.stats().getCurrentReplicaBytes());

            var forcedReplicaExpansionReleasable = indexingPressure.trackReplicaOperationExpansion(1024 * 16, true);
            assertEquals(1, indexingPressure.stats().getReplicaRejections());
            assertEquals(1024 * 19, indexingPressure.stats().getCurrentReplicaBytes());
            forcedReplicaExpansionReleasable.close();
        }

        assertEquals(1024 * 19, indexingPressure.stats().getTotalReplicaBytes());
        // ensure that the expansion does not increase the number of ops twice
        assertEquals(2, indexingPressure.stats().getTotalReplicaOps());
    }

    public void testLargeOperationRejections() {
        IndexingPressure indexingPressure = new IndexingPressure(settings);
        // max operation size is 128b
        indexingPressure.checkLargestPrimaryOperationIsWithinLimits(1, 1, false);
        assertEquals(0L, indexingPressure.stats().getLargeOpsRejections());
        assertEquals(0L, indexingPressure.stats().getTotalLargeRejectedOpsBytes());
        assertEquals(0L, indexingPressure.stats().getPrimaryRejections());
        assertEquals(0L, indexingPressure.stats().getPrimaryDocumentRejections());

        expectThrows(
            EsRejectedExecutionException.class,
            () -> indexingPressure.checkLargestPrimaryOperationIsWithinLimits(12, 2048, false)
        );
        assertEquals(1L, indexingPressure.stats().getLargeOpsRejections());
        assertEquals(1024L * 2, indexingPressure.stats().getTotalLargeRejectedOpsBytes());
        assertEquals(1L, indexingPressure.stats().getPrimaryRejections());
        assertEquals(12L, indexingPressure.stats().getPrimaryDocumentRejections());

        indexingPressure.checkLargestPrimaryOperationIsWithinLimits(12, 2048, true);
        assertEquals(2L, indexingPressure.stats().getLargeOpsRejections());
        assertEquals(2 * 2048L, indexingPressure.stats().getTotalLargeRejectedOpsBytes());
        assertEquals(1L, indexingPressure.stats().getPrimaryRejections());
        assertEquals(12L, indexingPressure.stats().getPrimaryDocumentRejections());
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
