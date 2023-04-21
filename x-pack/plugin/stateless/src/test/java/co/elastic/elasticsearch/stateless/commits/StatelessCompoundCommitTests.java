/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.commits;

import org.apache.lucene.store.Directory;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;

public class StatelessCompoundCommitTests extends AbstractWireTestCase<StatelessCompoundCommit> {

    @Override
    protected StatelessCompoundCommit createTestInstance() {
        return new StatelessCompoundCommit(
            randomShardId(),
            randomNonZeroPositiveLong(),
            randomNonZeroPositiveLong(),
            randomNodeEphemeralId(),
            randomCommitFiles()
        );
    }

    @Override
    protected StatelessCompoundCommit mutateInstance(StatelessCompoundCommit instance) throws IOException {
        return switch (randomInt(4)) {
            case 0 -> new StatelessCompoundCommit(
                randomValueOtherThan(instance.shardId(), StatelessCompoundCommitTests::randomShardId),
                instance.generation(),
                instance.primaryTerm(),
                instance.nodeEphemeralId(),
                instance.commitFiles()
            );
            case 1 -> new StatelessCompoundCommit(
                instance.shardId(),
                randomValueOtherThan(instance.generation(), StatelessCompoundCommitTests::randomNonZeroPositiveLong),
                instance.primaryTerm(),
                instance.nodeEphemeralId(),
                instance.commitFiles()
            );
            case 2 -> new StatelessCompoundCommit(
                instance.shardId(),
                instance.generation(),
                randomValueOtherThan(instance.primaryTerm(), StatelessCompoundCommitTests::randomNonZeroPositiveLong),
                instance.nodeEphemeralId(),
                instance.commitFiles()
            );
            case 3 -> new StatelessCompoundCommit(
                instance.shardId(),
                instance.generation(),
                instance.primaryTerm(),
                randomValueOtherThan(instance.nodeEphemeralId(), StatelessCompoundCommitTests::randomNodeEphemeralId),
                instance.commitFiles()
            );
            case 4 -> new StatelessCompoundCommit(
                instance.shardId(),
                instance.generation(),
                instance.primaryTerm(),
                instance.nodeEphemeralId(),
                randomValueOtherThan(instance.commitFiles(), StatelessCompoundCommitTests::randomCommitFiles)
            );
            default -> throw new AssertionError("Unexpected value");
        };
    }

    @Override
    protected StatelessCompoundCommit copyInstance(StatelessCompoundCommit instance, TransportVersion version) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            StatelessCompoundCommit.Writer writer = new StatelessCompoundCommit.Writer(
                instance.shardId(),
                instance.generation(),
                instance.primaryTerm(),
                instance.nodeEphemeralId()
            );
            for (Map.Entry<String, BlobLocation> entry : instance.commitFiles().entrySet()) {
                writer.addReferencedBlobFile(entry.getKey(), entry.getValue());
            }
            writer.write(output, mock(Directory.class));
            try (StreamInput in = output.bytes().streamInput()) {
                return StatelessCompoundCommit.readFromStore(in);
            }
        }
    }

    public void testCorruption() throws Exception {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            StatelessCompoundCommit testInstance = createTestInstance();
            StatelessCompoundCommit.Writer writer = new StatelessCompoundCommit.Writer(
                testInstance.shardId(),
                testInstance.generation(),
                testInstance.primaryTerm(),
                testInstance.nodeEphemeralId()
            );
            for (Map.Entry<String, BlobLocation> entry : testInstance.commitFiles().entrySet()) {
                writer.addReferencedBlobFile(entry.getKey(), entry.getValue());
            }
            writer.write(output, mock(Directory.class));
            // flip one byte anywhere
            byte[] bytes = BytesReference.toBytes(output.bytes());
            int i = randomIntBetween(0, bytes.length - 1);
            bytes[i] = (byte) ~bytes[i];
            try (StreamInput in = new ByteArrayStreamInput(bytes)) {
                expectThrows(IOException.class, () -> StatelessCompoundCommit.readFromStore(in));
            }
        }
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
            .mapToObj(operand -> "file_" + operand)
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    s -> new BlobLocation(randomLongBetween(1, 10), randomAlphaOfLength(10), 0, randomLongBetween(100, 1000))
                )
            );
    }
}
