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

package co.elastic.elasticsearch.stateless.recovery;

import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGenerationTests;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.serverless.constants.ServerlessTransportVersions.REGISTER_BATCHED_COMPOUND_COMMIT_ON_SEARCH_SHARD_RECOVERY;
import static co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGenerationTests.randomPrimaryTermAndGeneration;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RegisterCommitResponseSerializationTests extends AbstractWireSerializingTestCase<RegisterCommitResponse> {

    @Override
    protected Writeable.Reader<RegisterCommitResponse> instanceReader() {
        return RegisterCommitResponse::new;
    }

    @Override
    protected RegisterCommitResponse createTestInstance() {
        return new RegisterCommitResponse(randomPrimaryTermAndGeneration(), randomBoolean() ? randomCompoundCommit() : null);
    }

    @Override
    protected RegisterCommitResponse mutateInstance(RegisterCommitResponse instance) throws IOException {
        int i = randomIntBetween(0, 1);
        return switch (i) {
            case 0 -> new RegisterCommitResponse(
                randomValueOtherThan(
                    instance.getLatestUploadedBatchedCompoundCommitTermAndGen(),
                    PrimaryTermAndGenerationTests::randomPrimaryTermAndGeneration
                ),
                instance.getCompoundCommit()
            );
            case 1 -> new RegisterCommitResponse(
                instance.getLatestUploadedBatchedCompoundCommitTermAndGen(),
                randomValueOtherThan(instance.getCompoundCommit(), () -> randomBoolean() ? randomCompoundCommit() : null)
            );
            default -> throw new IllegalStateException("Unexpected value " + i);
        };
    }

    public void testSerializationBwc() throws IOException {
        final var instance = createTestInstance();
        final TransportVersion previousVersion = TransportVersionUtils.getPreviousVersion(
            REGISTER_BATCHED_COMPOUND_COMMIT_ON_SEARCH_SHARD_RECOVERY
        );

        var deserialized = copyInstance(instance, previousVersion);
        try {
            assertThat(
                deserialized.getLatestUploadedBatchedCompoundCommitTermAndGen(),
                equalTo(instance.getLatestUploadedBatchedCompoundCommitTermAndGen())
            );
            assertThat(deserialized.getCompoundCommit(), nullValue());
        } finally {
            dispose(deserialized);
        }
    }

    private static StatelessCompoundCommit randomCompoundCommit() {
        Map<String, BlobLocation> commitFiles = randomCommitFiles();
        return new StatelessCompoundCommit(
            randomShardId(),
            new PrimaryTermAndGeneration(randomNonZeroPositiveLong(), randomNonZeroPositiveLong()),
            randomNonZeroPositiveLong(),
            randomNodeEphemeralId(),
            commitFiles,
            randomNonZeroPositiveLong(),
            Set.copyOf(randomSubsetOf(commitFiles.keySet()))
        );
    }

    private static ShardId randomShardId() {
        return new ShardId(randomAlphaOfLength(20), UUIDs.randomBase64UUID(), randomIntBetween(0, 25));
    }

    private static Long randomNonZeroPositiveLong() {
        return randomLongBetween(1L, Long.MAX_VALUE - 1L);
    }

    private static String randomNodeEphemeralId() {
        return randomAlphaOfLength(10);
    }

    private static Map<String, BlobLocation> randomCommitFiles() {
        final int entries = randomInt(50);
        if (entries == 0) {
            return Map.of();
        }
        return IntStream.range(0, entries + 1)
            .mapToObj(operand -> UUIDs.randomBase64UUID())
            .collect(Collectors.toMap(Function.identity(), s -> {
                long fileLength = randomLongBetween(100, 1000);
                long offset = randomLongBetween(0, 200);
                return new BlobLocation(randomLongBetween(1, 10), randomAlphaOfLength(10), offset, fileLength);
            }));
    }
}
