/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.stateless.commits.BlobLocationTestUtils.createBlobLocation;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomCompoundCommit;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomTimestampFieldValueRange;

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
