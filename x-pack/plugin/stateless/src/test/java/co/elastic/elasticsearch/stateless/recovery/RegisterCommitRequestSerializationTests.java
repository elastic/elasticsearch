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

import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGenerationTests;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

import static co.elastic.elasticsearch.serverless.constants.ServerlessTransportVersions.REGISTER_BATCHED_COMPOUND_COMMIT_ON_SEARCH_SHARD_RECOVERY;
import static co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGenerationTests.randomPrimaryTermAndGeneration;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RegisterCommitRequestSerializationTests extends AbstractWireSerializingTestCase<RegisterCommitRequest> {

    @Override
    protected Writeable.Reader<RegisterCommitRequest> instanceReader() {
        return RegisterCommitRequest::new;
    }

    @Override
    protected RegisterCommitRequest createTestInstance() {
        return new RegisterCommitRequest(
            randomPrimaryTermAndGeneration(),
            randomPrimaryTermAndGeneration(),
            randomShardId(),
            randomIdentifier(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected RegisterCommitRequest mutateInstance(RegisterCommitRequest instance) throws IOException {
        int i = randomIntBetween(0, 4);
        return switch (i) {
            case 0 -> new RegisterCommitRequest(
                randomValueOtherThan(
                    instance.getBatchedCompoundCommitPrimaryTermAndGeneration(),
                    PrimaryTermAndGenerationTests::randomPrimaryTermAndGeneration
                ),
                instance.getCompoundCommitPrimaryTermAndGeneration(),
                instance.getShardId(),
                instance.getNodeId(),
                instance.getClusterStateVersion()
            );
            case 1 -> new RegisterCommitRequest(
                instance.getBatchedCompoundCommitPrimaryTermAndGeneration(),
                randomValueOtherThan(
                    instance.getCompoundCommitPrimaryTermAndGeneration(),
                    PrimaryTermAndGenerationTests::randomPrimaryTermAndGeneration
                ),
                instance.getShardId(),
                instance.getNodeId(),
                instance.getClusterStateVersion()
            );
            case 2 -> new RegisterCommitRequest(
                instance.getBatchedCompoundCommitPrimaryTermAndGeneration(),
                instance.getCompoundCommitPrimaryTermAndGeneration(),
                randomValueOtherThan(instance.getShardId(), RegisterCommitRequestSerializationTests::randomShardId),
                instance.getNodeId(),
                instance.getClusterStateVersion()
            );
            case 3 -> new RegisterCommitRequest(
                instance.getBatchedCompoundCommitPrimaryTermAndGeneration(),
                instance.getCompoundCommitPrimaryTermAndGeneration(),
                instance.getShardId(),
                randomValueOtherThan(instance.getNodeId(), ESTestCase::randomIdentifier),
                instance.getClusterStateVersion()
            );
            case 4 -> new RegisterCommitRequest(
                instance.getBatchedCompoundCommitPrimaryTermAndGeneration(),
                instance.getCompoundCommitPrimaryTermAndGeneration(),
                instance.getShardId(),
                instance.getNodeId(),
                randomValueOtherThan(instance.getClusterStateVersion(), ESTestCase::randomNonNegativeLong)
            );
            default -> throw new IllegalStateException("Unexpected value " + i);
        };
    }

    public static ShardId randomShardId() {
        return new ShardId(randomAlphaOfLength(20), UUIDs.randomBase64UUID(), randomIntBetween(0, 25));
    }

    public void testSerializationNewToOld() throws IOException {
        final var instance = createTestInstance();
        final TransportVersion previousVersion = TransportVersionUtils.getPreviousVersion(
            REGISTER_BATCHED_COMPOUND_COMMIT_ON_SEARCH_SHARD_RECOVERY
        );

        var deserialized = copyInstance(instance, previousVersion);
        try {
            assertThat(deserialized.getBatchedCompoundCommitPrimaryTermAndGeneration(), nullValue());
            assertThat(
                deserialized.getCompoundCommitPrimaryTermAndGeneration(),
                equalTo(instance.getCompoundCommitPrimaryTermAndGeneration())
            );
            assertThat(deserialized.getShardId(), equalTo(instance.getShardId()));
            assertThat(deserialized.getNodeId(), equalTo(instance.getNodeId()));
            assertThat(deserialized.getClusterStateVersion(), equalTo(instance.getClusterStateVersion()));
        } finally {
            dispose(deserialized);
        }
    }

    public void testDeserializationOldToNew() throws IOException {
        try (var out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersionUtils.getNextVersion(REGISTER_BATCHED_COMPOUND_COMMIT_ON_SEARCH_SHARD_RECOVERY, true));
            final var parentTaskId = new TaskId(randomIdentifier(), randomNonNegativeLong());
            final var instance = new RegisterCommitRequest(
                null,
                randomPrimaryTermAndGeneration(),
                randomShardId(),
                randomIdentifier(),
                randomNonNegativeLong()
            );

            // old logic to serialize RegisterCommitRequest without BCC generation
            parentTaskId.writeTo(out);
            instance.getCompoundCommitPrimaryTermAndGeneration().writeTo(out);
            instance.getShardId().writeTo(out);
            out.writeString(instance.getNodeId());
            out.writeZLong(instance.getClusterStateVersion());

            try (var in = out.bytes().streamInput()) {
                in.setTransportVersion(TransportVersionUtils.getPreviousVersion(REGISTER_BATCHED_COMPOUND_COMMIT_ON_SEARCH_SHARD_RECOVERY));
                var deserialized = instanceReader().read(in);
                try {
                    assertThat(deserialized.getBatchedCompoundCommitPrimaryTermAndGeneration(), nullValue());
                    assertThat(
                        deserialized.getCompoundCommitPrimaryTermAndGeneration(),
                        equalTo(instance.getCompoundCommitPrimaryTermAndGeneration())
                    );
                    assertThat(deserialized.getShardId(), equalTo(instance.getShardId()));
                    assertThat(deserialized.getNodeId(), equalTo(instance.getNodeId()));
                    assertThat(deserialized.getClusterStateVersion(), equalTo(instance.getClusterStateVersion()));

                } finally {
                    dispose(deserialized);
                }
            }
        }
    }
}
