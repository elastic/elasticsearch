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

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGenerationTests.randomPrimaryTermAndGeneration;

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
}
