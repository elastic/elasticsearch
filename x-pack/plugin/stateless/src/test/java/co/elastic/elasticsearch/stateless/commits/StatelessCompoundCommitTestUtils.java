/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.stateless.commits.BlobLocationTestUtils.createBlobLocation;
import static org.apache.lucene.tests.util.LuceneTestCase.rarely;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomLongBetween;
import static org.elasticsearch.test.ESTestCase.randomSubsetOf;

public final class StatelessCompoundCommitTestUtils {

    private StatelessCompoundCommitTestUtils() {}

    public static StatelessCompoundCommit randomCompoundCommit() {
        return randomCompoundCommit(
            randomShardId(),
            new PrimaryTermAndGeneration(randomNonZeroPositiveLong(), randomNonZeroPositiveLong())
        );
    }

    public static StatelessCompoundCommit randomCompoundCommit(ShardId shardId, PrimaryTermAndGeneration termAndGeneration) {
        Map<String, BlobLocation> commitFiles = randomCommitFiles();
        return new StatelessCompoundCommit(
            shardId,
            termAndGeneration,
            randomNonZeroPositiveLong(),
            randomNodeEphemeralId(),
            commitFiles,
            randomNonZeroPositiveLong(),
            Set.copyOf(randomSubsetOf(commitFiles.keySet())),
            randomNonZeroPositiveLong(),
            randomInternalFilesReplicatedRanges()
        );
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

    public static Map<String, BlobLocation> randomCommitFiles() {
        final int entries = randomInt(50);
        if (entries == 0) {
            return Map.of();
        }
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
