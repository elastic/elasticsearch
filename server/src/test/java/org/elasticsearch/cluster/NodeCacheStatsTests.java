/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.hamcrest.Matchers.containsString;

public class NodeCacheStatsTests extends AbstractWireSerializingTestCase<NodeCacheStats> {

    private static final long MAX_TEST_CACHE_SIZE = 1000L;
    private static final long MAX_TEST_BOOSTED_CACHE_COMMITMENT = 1000L;
    private static final long MIN_TEST_TOTAL_CACHE_COMMITMENT = 1001L;
    private static final long MAX_TEST_TOTAL_CACHE_COMMITMENT = 2000L;

    @Override
    protected Writeable.Reader<NodeCacheStats> instanceReader() {
        return NodeCacheStats::new;
    }

    @Override
    protected NodeCacheStats createTestInstance() {
        return randomNodeCacheStats();
    }

    @Override
    protected NodeCacheStats mutateInstance(NodeCacheStats instance) {
        return switch (between(0, 2)) {
            case 0 -> new NodeCacheStats(
                randomValueOtherThan(instance.cacheSizeInBytes(), () -> randomLongBetween(0, MAX_TEST_CACHE_SIZE)),
                instance.boostedCacheCommitmentInBytes(),
                instance.totalCacheCommitmentInBytes()
            );
            case 1 -> new NodeCacheStats(
                instance.cacheSizeInBytes(),
                randomValueOtherThan(
                    instance.boostedCacheCommitmentInBytes(),
                    () -> randomLongBetween(0, MAX_TEST_BOOSTED_CACHE_COMMITMENT)
                ),
                instance.totalCacheCommitmentInBytes()
            );
            case 2 -> new NodeCacheStats(
                instance.cacheSizeInBytes(),
                instance.boostedCacheCommitmentInBytes(),
                randomValueOtherThan(
                    instance.totalCacheCommitmentInBytes(),
                    () -> randomLongBetween(MIN_TEST_TOTAL_CACHE_COMMITMENT, MAX_TEST_TOTAL_CACHE_COMMITMENT)
                )
            );
            default -> throw new AssertionError("unexpected branch");
        };
    }

    public void testRejectsNegativeValues() {
        AssertionError cacheSizeError = expectThrows(AssertionError.class, () -> new NodeCacheStats(-1L, 0L, 0L));
        assertThat(cacheSizeError.getMessage(), containsString("cacheSizeInBytes must be non-negative"));

        AssertionError boostedCommitmentError = expectThrows(AssertionError.class, () -> new NodeCacheStats(0L, -1L, 0L));
        assertThat(boostedCommitmentError.getMessage(), containsString("boostedCacheCommitmentInBytes must be non-negative"));

        AssertionError totalCommitmentError = expectThrows(AssertionError.class, () -> new NodeCacheStats(0L, 0L, -1L));
        assertThat(totalCommitmentError.getMessage(), containsString("totalCacheCommitmentInBytes must be non-negative"));
    }

    public void testRejectsTotalCommitmentBelowBoostedCommitment() {
        AssertionError error = expectThrows(AssertionError.class, () -> new NodeCacheStats(0L, 2L, 1L));
        assertThat(error.getMessage(), containsString("totalCacheCommitmentInBytes must be >= boostedCacheCommitmentInBytes"));
    }

    static NodeCacheStats randomNodeCacheStats() {
        long boostedCommitmentInBytes = randomLongBetween(0, MAX_TEST_BOOSTED_CACHE_COMMITMENT);
        long totalCommitmentInBytes = randomLongBetween(MIN_TEST_TOTAL_CACHE_COMMITMENT, MAX_TEST_TOTAL_CACHE_COMMITMENT);
        return new NodeCacheStats(randomLongBetween(0, MAX_TEST_CACHE_SIZE), boostedCommitmentInBytes, totalCommitmentInBytes);
    }
}
