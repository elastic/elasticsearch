/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit.TimestampFieldValueRange;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.lucene.tests.util.LuceneTestCase.rarely;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;
import static org.elasticsearch.test.ESTestCase.randomNonEmptySubsetOf;
import static org.elasticsearch.test.ESTestCase.randomValueOtherThanMany;
import static org.elasticsearch.xpack.stateless.commits.BlobLocationTestUtils.createBlobLocation;

public final class StatelessCompoundCommitTestUtils {

    private StatelessCompoundCommitTestUtils() {}

    public static StatelessCompoundCommit randomCompoundCommit() {
        return randomCompoundCommit(
            randomShardId(),
            new PrimaryTermAndGeneration(randomNonZeroPositiveLong(), randomNonZeroPositiveLong())
        );
    }

    public static StatelessCompoundCommit randomCompoundCommit(ShardId shardId, PrimaryTermAndGeneration termAndGeneration) {
        return randomCompoundCommit(shardId, termAndGeneration, randomBoolean());
    }

    public static StatelessCompoundCommit randomCompoundCommit(
        ShardId shardId,
        PrimaryTermAndGeneration termAndGeneration,
        boolean hollow
    ) {
        Map<String, BlobLocation> commitFiles = randomCommitFiles();
        if (hollow) {
            return StatelessCompoundCommit.newHollowStatelessCompoundCommit(
                shardId,
                termAndGeneration,
                commitFiles,
                randomNonZeroPositiveLong(),
                Set.copyOf(randomNonEmptySubsetOf(commitFiles.keySet())),
                randomNonZeroPositiveLong(),
                randomInternalFilesReplicatedRanges(),
                randomCommitFiles(),
                randomFrom(randomTimestampFieldValueRange(), null)
            );
        } else {
            return new StatelessCompoundCommit(
                shardId,
                termAndGeneration,
                randomNonZeroPositiveLong(),
                randomNodeEphemeralId(),
                commitFiles,
                randomNonZeroPositiveLong(),
                Set.copyOf(randomNonEmptySubsetOf(commitFiles.keySet())),
                randomNonZeroPositiveLong(),
                randomInternalFilesReplicatedRanges(),
                Map.of(),
                randomFrom(randomTimestampFieldValueRange(), null)
            );
        }
    }

    public static ShardId randomShardId() {
        return new ShardId(randomAlphaOfLength(20), UUIDs.randomBase64UUID(), randomIntBetween(0, 25));
    }

    public static Long randomNonZeroPositiveLong() {
        return randomLongBetween(1L, Long.MAX_VALUE - 1L);
    }

    public static String randomNodeEphemeralId() {
        return randomAlphaOfLength(10);
    }

    public static TimestampFieldValueRange randomTimestampFieldValueRange() {
        long minTimestamp = randomLongBetween(0L, Long.MAX_VALUE - 1L);
        return new TimestampFieldValueRange(
            minTimestamp,
            randomValueOtherThanMany(maxTimestamp -> maxTimestamp < minTimestamp, () -> randomLongBetween(0L, Long.MAX_VALUE))
        );
    }

    public static Map<String, BlobLocation> randomCommitFiles() {
        final int entries = randomIntBetween(1, 50);
        return IntStream.range(0, entries + 1)
            .mapToObj(operand -> UUIDs.randomBase64UUID())
            .collect(Collectors.toMap(Function.identity(), s -> {
                long fileLength = randomLongBetween(100, 1000);
                long offset = randomLongBetween(0, 200);
                return createBlobLocation(randomNonZeroPositiveLong(), randomLongBetween(1, 1000), offset, fileLength);
            }));
    }

    public static InternalFilesReplicatedRanges randomInternalFilesReplicatedRanges() {
        if (rarely()) {
            return InternalFilesReplicatedRanges.EMPTY;
        }
        int maxNumberOfRanges = randomIntBetween(1, 25);
        var replicatedRanges = new ArrayList<InternalFilesReplicatedRanges.InternalFileReplicatedRange>();
        long position = randomNonZeroPositiveLong();
        while (replicatedRanges.size() < maxNumberOfRanges && position < Long.MAX_VALUE) {
            position = randomLongBetween(position, Long.MAX_VALUE - 1L);
            short length = (short) Math.min(randomLongBetween(1L, Short.MAX_VALUE), Long.MAX_VALUE - position);
            replicatedRanges.add(new InternalFilesReplicatedRanges.InternalFileReplicatedRange(position, length));
            position += length;
        }
        return InternalFilesReplicatedRanges.from(replicatedRanges);
    }
}
