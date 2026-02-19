/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Objects;

/**
 * Wire serialization tests for {@link ClusterStateSerializationStats}.
 * Uses a wrapper type so that equality after round-trip is semantic (field-by-field),
 * since {@link ClusterStateSerializationStats} does not implement {@code equals} or {@code hashCode}.
 */
public class ClusterStateSerializationStatsWireSerializingTests extends AbstractWireSerializingTestCase<
    ClusterStateSerializationStatsWireSerializingTests.ClusterStateSerializationStatsWrapper> {

    @Override
    protected Writeable.Reader<ClusterStateSerializationStatsWrapper> instanceReader() {
        return ClusterStateSerializationStatsWrapper::new;
    }

    @Override
    protected ClusterStateSerializationStatsWrapper createTestInstance() {
        ClusterStateSerializationStats stats = new ClusterStateSerializationStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
        return new ClusterStateSerializationStatsWrapper(stats);
    }

    @Override
    protected ClusterStateSerializationStatsWrapper mutateInstance(ClusterStateSerializationStatsWrapper instance) throws IOException {
        ClusterStateSerializationStats stats = instance.stats;
        long fullStateCount = stats.getFullStateCount();
        long totalUncompressedFullStateBytes = stats.getTotalUncompressedFullStateBytes();
        long totalCompressedFullStateBytes = stats.getTotalCompressedFullStateBytes();
        long diffCount = stats.getDiffCount();
        long totalUncompressedDiffBytes = stats.getTotalUncompressedDiffBytes();
        long totalCompressedDiffBytes = stats.getTotalCompressedDiffBytes();

        int field = between(0, 5);
        switch (field) {
            case 0 -> fullStateCount = randomValueOtherThan(fullStateCount, () -> randomNonNegativeLong());
            case 1 -> totalUncompressedFullStateBytes = randomValueOtherThan(
                totalUncompressedFullStateBytes,
                () -> randomNonNegativeLong()
            );
            case 2 -> totalCompressedFullStateBytes = randomValueOtherThan(totalCompressedFullStateBytes, () -> randomNonNegativeLong());
            case 3 -> diffCount = randomValueOtherThan(diffCount, () -> randomNonNegativeLong());
            case 4 -> totalUncompressedDiffBytes = randomValueOtherThan(totalUncompressedDiffBytes, () -> randomNonNegativeLong());
            default -> totalCompressedDiffBytes = randomValueOtherThan(totalCompressedDiffBytes, () -> randomNonNegativeLong());
        }

        ClusterStateSerializationStats mutatedStats = new ClusterStateSerializationStats(
            fullStateCount,
            totalUncompressedFullStateBytes,
            totalCompressedFullStateBytes,
            diffCount,
            totalUncompressedDiffBytes,
            totalCompressedDiffBytes
        );
        return new ClusterStateSerializationStatsWrapper(mutatedStats);
    }

    @Override
    protected void assertEqualInstances(
        ClusterStateSerializationStatsWrapper expectedInstance,
        ClusterStateSerializationStatsWrapper newInstance
    ) {
        assertNotSame(expectedInstance, newInstance);
        ClusterStateSerializationStats expectedStats = expectedInstance.stats;
        ClusterStateSerializationStats newStats = newInstance.stats;
        assertEquals(expectedStats.getFullStateCount(), newStats.getFullStateCount());
        assertEquals(expectedStats.getTotalUncompressedFullStateBytes(), newStats.getTotalUncompressedFullStateBytes());
        assertEquals(expectedStats.getTotalCompressedFullStateBytes(), newStats.getTotalCompressedFullStateBytes());
        assertEquals(expectedStats.getDiffCount(), newStats.getDiffCount());
        assertEquals(expectedStats.getTotalUncompressedDiffBytes(), newStats.getTotalUncompressedDiffBytes());
        assertEquals(expectedStats.getTotalCompressedDiffBytes(), newStats.getTotalCompressedDiffBytes());
    }

    /**
     * Wrapper around {@link ClusterStateSerializationStats} that implements semantic equality and hashCode
     * so that round-trip serialization passes.
     */
    static class ClusterStateSerializationStatsWrapper implements Writeable {
        private final ClusterStateSerializationStats stats;

        ClusterStateSerializationStatsWrapper(ClusterStateSerializationStats stats) {
            this.stats = stats;
        }

        ClusterStateSerializationStatsWrapper(StreamInput in) throws IOException {
            this.stats = new ClusterStateSerializationStats(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            stats.writeTo(out);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (other == null || getClass() != other.getClass()) return false;
            ClusterStateSerializationStatsWrapper that = (ClusterStateSerializationStatsWrapper) other;
            return statsEquals(stats, that.stats);
        }

        @Override
        public int hashCode() {
            return statsHashCode(stats);
        }

        private static boolean statsEquals(ClusterStateSerializationStats first, ClusterStateSerializationStats second) {
            if (first == second) return true;
            if (first == null || second == null) return false;
            return first.getFullStateCount() == second.getFullStateCount()
                && first.getTotalUncompressedFullStateBytes() == second.getTotalUncompressedFullStateBytes()
                && first.getTotalCompressedFullStateBytes() == second.getTotalCompressedFullStateBytes()
                && first.getDiffCount() == second.getDiffCount()
                && first.getTotalUncompressedDiffBytes() == second.getTotalUncompressedDiffBytes()
                && first.getTotalCompressedDiffBytes() == second.getTotalCompressedDiffBytes();
        }

        private static int statsHashCode(ClusterStateSerializationStats stats) {
            if (stats == null) return 0;
            return Objects.hash(
                stats.getFullStateCount(),
                stats.getTotalUncompressedFullStateBytes(),
                stats.getTotalCompressedFullStateBytes(),
                stats.getDiffCount(),
                stats.getTotalUncompressedDiffBytes(),
                stats.getTotalCompressedDiffBytes()
            );
        }
    }
}
