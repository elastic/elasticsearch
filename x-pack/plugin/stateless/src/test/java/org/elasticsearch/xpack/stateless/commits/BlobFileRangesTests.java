/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;

import static org.elasticsearch.xpack.stateless.TestUtils.getCommitWithInternalFilesReplicatedRanges;
import static org.elasticsearch.xpack.stateless.commits.BlobLocationTestUtils.createBlobLocation;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomCompoundCommit;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomNonZeroPositiveLong;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomShardId;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomTimestampFieldValueRange;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

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

    public void testReconcileWithTimestamps() {
        final var location = createBlobLocation(1L, 1L, 0L, 100L);
        final var olderRange = new StatelessCompoundCommit.TimestampFieldValueRange(100L, 200L); // midpoint 150
        final var newerRange = new StatelessCompoundCommit.TimestampFieldValueRange(300L, 400L); // midpoint 350

        final var noTs = new BlobFileRanges(location);
        final var withOlderTs = new BlobFileRanges(location, olderRange);
        final var withNewerTs = new BlobFileRanges(location, newerRange);

        // neither has a timestamp: result has no timestamp
        assertThat(noTs.reconcileWith(noTs).timestampRange(), nullValue());

        // only existing has a timestamp: existing wins
        assertThat(withOlderTs.reconcileWith(noTs).timestampRange(), equalTo(olderRange));

        // only incoming has a timestamp: incoming wins
        assertThat(noTs.reconcileWith(withOlderTs).timestampRange(), equalTo(olderRange));

        // existing is newer: existing wins
        assertThat(withNewerTs.reconcileWith(withOlderTs).timestampRange(), equalTo(newerRange));

        // incoming is newer: incoming wins
        assertThat(withOlderTs.reconcileWith(withNewerTs).timestampRange(), equalTo(newerRange));

        // equal midpoints: existing wins (tie-break preserves existing object)
        final var sameAsOlder = new BlobFileRanges(location, new StatelessCompoundCommit.TimestampFieldValueRange(100L, 200L));
        assertThat(withOlderTs.reconcileWith(sameAsOlder).timestampRange(), sameInstance(olderRange));
    }

    public void testReconcileWithReplicatedRanges() {
        final var withReplicated = randomBlobFileRangesWithReplicatedRanges();
        final var noReplicated = new BlobFileRanges(withReplicated.blobLocation());
        final var anotherNoReplicated = new BlobFileRanges(withReplicated.blobLocation());
        final var expectedCopy = withReplicated.locationOfFirstReplicatedContents();

        // neither has replicated ranges: result has none
        assertThat(noReplicated.reconcileWith(anotherNoReplicated).hasReplicatedRanges(), is(false));

        // only existing has replicated ranges: they are preserved
        assertThat(withReplicated.reconcileWith(noReplicated).hasReplicatedRanges(), is(true));
        assertThat(withReplicated.reconcileWith(noReplicated).locationOfFirstReplicatedContents(), equalTo(expectedCopy));

        // only incoming has replicated ranges: they are adopted
        assertThat(noReplicated.reconcileWith(withReplicated).hasReplicatedRanges(), is(true));
        assertThat(noReplicated.reconcileWith(withReplicated).locationOfFirstReplicatedContents(), equalTo(expectedCopy));

        // both have the same replicated ranges (reconcile with self): ranges are kept
        assertThat(withReplicated.reconcileWith(withReplicated).hasReplicatedRanges(), is(true));
        assertThat(withReplicated.reconcileWith(withReplicated).locationOfFirstReplicatedContents(), equalTo(expectedCopy));
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

    private static BlobFileRanges randomBlobFileRangesWithReplicatedRanges() {
        final long primaryTerm = randomNonZeroPositiveLong();
        final long generation = randomNonZeroPositiveLong();
        final var cc = getCommitWithInternalFilesReplicatedRanges(
            randomShardId(),
            new BlobFile(StatelessCompoundCommit.PREFIX + generation, new PrimaryTermAndGeneration(primaryTerm, generation)),
            "_na_",
            0,
            ByteSizeValue.ofKb(4).getBytes()
        );
        final var blobFileRangesMap = BlobFileRanges.computeBlobFileRanges(true, cc, 0L, cc.internalFiles());
        return randomFrom(blobFileRangesMap.values());
    }
}
