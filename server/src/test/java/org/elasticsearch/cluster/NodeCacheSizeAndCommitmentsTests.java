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
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class NodeCacheSizeAndCommitmentsTests extends AbstractWireSerializingTestCase<NodeCacheSizeAndCommitments> {

    @Override
    protected Writeable.Reader<NodeCacheSizeAndCommitments> instanceReader() {
        return NodeCacheSizeAndCommitments::new;
    }

    @Override
    protected NodeCacheSizeAndCommitments createTestInstance() {
        return randomNodeCacheSizeAndCommitments();
    }

    @Override
    protected NodeCacheSizeAndCommitments mutateInstance(NodeCacheSizeAndCommitments instance) {
        return switch (between(0, 2)) {
            case 0 -> new NodeCacheSizeAndCommitments(
                randomValueOtherThan(instance.cacheSizeInBytes(), NodeCacheSizeAndCommitmentsTests::randomNonNegativeLong),
                instance.boostedCacheCommitmentInBytes(),
                instance.unboostedCacheCommitmentInBytes()
            );
            case 1 -> new NodeCacheSizeAndCommitments(
                instance.cacheSizeInBytes(),
                randomValueOtherThan(instance.boostedCacheCommitmentInBytes(), NodeCacheSizeAndCommitmentsTests::randomNonNegativeLong),
                instance.unboostedCacheCommitmentInBytes()
            );
            case 2 -> new NodeCacheSizeAndCommitments(
                instance.cacheSizeInBytes(),
                instance.boostedCacheCommitmentInBytes(),
                randomValueOtherThan(instance.unboostedCacheCommitmentInBytes(), NodeCacheSizeAndCommitmentsTests::randomNonNegativeLong)
            );
            default -> throw new AssertionError("unexpected branch");
        };
    }

    public void testSpareCapacityBytes() {
        final long cacheSize = 1000L;
        final RatioValue watermark = RatioValue.ofPercent(75);
        final long threshold = (long) (cacheSize * watermark.getAsRatio()); // 750

        // Below the threshold: spare equals the gap.
        final var belowThreshold = new NodeCacheSizeAndCommitments(cacheSize, 500L, 0L);
        assertThat(belowThreshold.spareCapacityBytes(500L, watermark), equalTo(250L));

        // Exactly at the threshold: spare is zero.
        final var atThreshold = new NodeCacheSizeAndCommitments(cacheSize, threshold, 0L);
        assertThat(atThreshold.spareCapacityBytes(threshold, watermark), equalTo(0L));

        // Above the threshold: spare is clamped to zero, never negative.
        final var aboveThreshold = new NodeCacheSizeAndCommitments(cacheSize, threshold + 1, 0L);
        assertThat(aboveThreshold.spareCapacityBytes(threshold + 1, watermark), equalTo(0L));

        // Consistency with exceedsWatermark: a node that does not exceed the watermark has positive spare; one that
        // does has zero spare.
        final var instance = randomNodeCacheSizeAndCommitments();
        final long commitmentBytes = randomNonNegativeLong();
        final long spare = instance.spareCapacityBytes(commitmentBytes, watermark);
        if (instance.exceedsWatermark(commitmentBytes, watermark)) {
            assertThat(spare, equalTo(0L));
        } else {
            assertThat(spare, greaterThanOrEqualTo(0L));
        }
    }

    public void testRejectsNegativeValues() {
        AssertionError cacheSizeError = expectThrows(AssertionError.class, () -> new NodeCacheSizeAndCommitments(-1L, 0L, 0L));
        assertThat(cacheSizeError.getMessage(), containsString("cacheSizeInBytes must be non-negative"));

        AssertionError boostedCommitmentError = expectThrows(AssertionError.class, () -> new NodeCacheSizeAndCommitments(0L, -1L, 0L));
        assertThat(boostedCommitmentError.getMessage(), containsString("boostedCacheCommitmentInBytes must be non-negative"));

        AssertionError unboostedCommitmentError = expectThrows(AssertionError.class, () -> new NodeCacheSizeAndCommitments(0L, 0L, -1L));
        assertThat(unboostedCommitmentError.getMessage(), containsString("unboostedCacheCommitmentInBytes must be non-negative"));
    }

    static NodeCacheSizeAndCommitments randomNodeCacheSizeAndCommitments() {
        return new NodeCacheSizeAndCommitments(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }
}
