/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.stateless.commits.BlobLocationTestUtils.createBlobLocation;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomCompoundCommit;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomTimestampFieldValueRange;
import static org.hamcrest.Matchers.equalTo;

public class BlobFileRangesTests extends AbstractWireSerializingTestCase<BlobFileRanges> {

    @Override
    protected Writeable.Reader<BlobFileRanges> instanceReader() {
        return BlobFileRanges::new;
    }

    @Override
    protected BlobFileRanges createTestInstance() {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> new BlobFileRanges(randomBlobLocation());
            case 1 -> new BlobFileRanges(randomBlobLocation(), randomBoolean() ? randomTimestampFieldValueRange() : null);
            case 2 -> randomBlobFileRangesFromCommit();
            default -> throw new IllegalStateException("unreachable");
        };
    }

    @Override
    protected BlobFileRanges mutateInstance(BlobFileRanges instance) throws IOException {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> new BlobFileRanges(
                createBlobLocation(
                    randomValueOtherThan(instance.primaryTerm(), () -> randomLongBetween(1, 10)),
                    randomLongBetween(1, 1000),
                    randomLongBetween(0, 100),
                    randomLongBetween(100, 1000)
                )
            );
            case 1 -> new BlobFileRanges(
                instance.blobLocation(),
                randomValueOtherThan(instance.timestampRange(), () -> randomBoolean() ? randomTimestampFieldValueRange() : null)
            );
            case 2 -> instance.hasReplicatedRanges()
                ? new BlobFileRanges(instance.blobLocation(), instance.timestampRange())
                : randomValueOtherThan(instance, BlobFileRangesTests::randomBlobFileRangesFromCommit);
            default -> throw new IllegalStateException("unreachable");
        };
    }

    public void testMidpointMillisOrUnknownForCache() {
        assertThat(
            "a null timestamp range has no representable timestamp, so it resolves to UNKNOWN_TIMESTAMP",
            BlobFileRanges.midpointMillisOrUnknownForCache(null),
            equalTo(SharedBlobCacheService.UNKNOWN_TIMESTAMP)
        );
        assertThat(
            "a positive range resolves to its arithmetic midpoint",
            BlobFileRanges.midpointMillisOrUnknownForCache(new StatelessCompoundCommit.TimestampFieldValueRange(1000L, 3000L)),
            equalTo(2000L)
        );
        assertThat(
            "a single-point positive range resolves to that point",
            BlobFileRanges.midpointMillisOrUnknownForCache(new StatelessCompoundCommit.TimestampFieldValueRange(1L, 1L)),
            equalTo(1L)
        );
        assertThat(
            "a zero midpoint (content at the epoch) is floored to the oldest representable instant",
            BlobFileRanges.midpointMillisOrUnknownForCache(new StatelessCompoundCommit.TimestampFieldValueRange(0L, 0L)),
            equalTo(1L)
        );
        assertThat(
            "a negative midpoint (content before the epoch) is floored to the oldest representable instant",
            BlobFileRanges.midpointMillisOrUnknownForCache(new StatelessCompoundCommit.TimestampFieldValueRange(-3000L, -1000L)),
            equalTo(1L)
        );
        assertThat(
            "a range whose midpoint would collide with the UNKNOWN_TIMESTAMP sentinel (-1) is floored, not treated as unknown",
            BlobFileRanges.midpointMillisOrUnknownForCache(new StatelessCompoundCommit.TimestampFieldValueRange(-2L, 0L)),
            equalTo(1L)
        );
    }

    private static BlobLocation randomBlobLocation() {
        return createBlobLocation(
            randomLongBetween(1, 10),
            randomLongBetween(1, 1000),
            randomLongBetween(0, 100),
            randomLongBetween(100, 1000)
        );
    }

    private static BlobFileRanges randomBlobFileRangesFromCommit() {
        final var compoundCommit = randomCompoundCommit();
        final var blobFileRangesMap = BlobFileRanges.computeBlobFileRanges(true, compoundCommit, 0L, compoundCommit.internalFiles());
        return randomFrom(blobFileRangesMap.values());
    }
}
