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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.PositionTrackingOutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.in;

public class StatelessCompoundCommitTests extends AbstractWireSerializingTestCase<StatelessCompoundCommit> {

    @Override
    protected StatelessCompoundCommit createTestInstance() {
        return new StatelessCompoundCommit(
            randomShardId(),
            new PrimaryTermAndGeneration(randomNonZeroPositiveLong(), randomNonZeroPositiveLong()),
            randomNonZeroPositiveLong(),
            randomNodeEphemeralId(),
            randomCommitFiles(),
            randomNonZeroPositiveLong()
        );
    }

    @Override
    protected StatelessCompoundCommit mutateInstance(StatelessCompoundCommit instance) throws IOException {
        return switch (randomInt(5)) {
            case 0 -> new StatelessCompoundCommit(
                randomValueOtherThan(instance.shardId(), StatelessCompoundCommitTests::randomShardId),
                instance.primaryTermAndGeneration(),
                instance.translogRecoveryStartFile(),
                instance.nodeEphemeralId(),
                instance.commitFiles(),
                instance.sizeInBytes()
            );
            case 1 -> new StatelessCompoundCommit(
                instance.shardId(),
                randomValueOtherThan(
                    instance.primaryTermAndGeneration(),
                    () -> new PrimaryTermAndGeneration(randomNonZeroPositiveLong(), randomNonZeroPositiveLong())
                ),
                instance.translogRecoveryStartFile(),
                instance.nodeEphemeralId(),
                instance.commitFiles(),
                instance.sizeInBytes()
            );
            case 2 -> new StatelessCompoundCommit(
                instance.shardId(),
                instance.primaryTermAndGeneration(),
                randomValueOtherThan(instance.translogRecoveryStartFile(), StatelessCompoundCommitTests::randomNonZeroPositiveLong),
                instance.nodeEphemeralId(),
                instance.commitFiles(),
                instance.sizeInBytes()
            );
            case 3 -> new StatelessCompoundCommit(
                instance.shardId(),
                instance.primaryTermAndGeneration(),
                instance.translogRecoveryStartFile(),
                randomValueOtherThan(instance.nodeEphemeralId(), StatelessCompoundCommitTests::randomNodeEphemeralId),
                instance.commitFiles(),
                instance.sizeInBytes()
            );
            case 4 -> new StatelessCompoundCommit(
                instance.shardId(),
                instance.primaryTermAndGeneration(),
                instance.translogRecoveryStartFile(),
                instance.nodeEphemeralId(),
                randomValueOtherThan(instance.commitFiles(), StatelessCompoundCommitTests::randomCommitFiles),
                instance.sizeInBytes()
            );
            case 5 -> new StatelessCompoundCommit(
                instance.shardId(),
                instance.primaryTermAndGeneration(),
                instance.translogRecoveryStartFile(),
                instance.nodeEphemeralId(),
                instance.commitFiles(),
                randomValueOtherThan(instance.sizeInBytes(), StatelessCompoundCommitTests::randomNonZeroPositiveLong)
            );
            default -> throw new AssertionError("Unexpected value");
        };
    }

    @Override
    protected Writeable.Reader<StatelessCompoundCommit> instanceReader() {
        return StatelessCompoundCommit::readFromTransport;
    }

    public void testStoreSerialization() throws IOException {
        StatelessCompoundCommit instance = createTestInstance();

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            StatelessCompoundCommit.Writer writer = new StatelessCompoundCommit.Writer(
                instance.shardId(),
                instance.generation(),
                instance.primaryTerm(),
                instance.translogRecoveryStartFile(),
                instance.nodeEphemeralId()
            );
            for (Map.Entry<String, BlobLocation> entry : instance.commitFiles().entrySet()) {
                writer.addReferencedBlobFile(entry.getKey(), entry.getValue());
            }

            int internalFilesCount = randomInt(5);

            ArrayList<Tuple<String, Long>> internalFiles = new ArrayList<>();
            for (int i = 0; i < internalFilesCount; ++i) {
                Tuple<String, Long> tuple = new Tuple<>(randomAlphaOfLength(10), randomLongBetween(10, 100));
                internalFiles.add(tuple);
                writer.addInternalFile(tuple.v1(), tuple.v2());
            }
            PositionTrackingOutputStreamStreamOutput positionTracking = new PositionTrackingOutputStreamStreamOutput(output);
            writer.writeHeaderToStore(positionTracking, StatelessCompoundCommit.CURRENT_VERSION);
            long headerSize = positionTracking.position();

            long commitFileLength = internalFiles.stream().mapToLong(Tuple::v2).sum() + headerSize;
            Map<String, BlobLocation> commitFilesToModify = new HashMap<>(instance.commitFiles());
            long offset = headerSize;
            for (Tuple<String, Long> internalFile : internalFiles) {
                BlobLocation blobLocation = new BlobLocation(
                    instance.primaryTerm(),
                    StatelessCompoundCommit.blobNameFromGeneration(instance.generation()),
                    commitFileLength,
                    offset,
                    internalFile.v2()
                );
                offset += internalFile.v2();
                commitFilesToModify.put(internalFile.v1(), blobLocation);
            }

            StatelessCompoundCommit withInternalFiles = new StatelessCompoundCommit(
                instance.shardId(),
                instance.primaryTermAndGeneration(),
                instance.translogRecoveryStartFile(),
                instance.nodeEphemeralId(),
                commitFilesToModify,
                commitFileLength
            );

            try (StreamInput in = output.bytes().streamInput()) {
                StatelessCompoundCommit compoundCommit = StatelessCompoundCommit.readFromStore(in);
                assertEqualInstances(withInternalFiles, compoundCommit);
            }
        }
    }

    public void testStoreVersionCompatibility() throws Exception {
        StatelessCompoundCommit testInstance = createTestInstance();
        Map<String, BlobLocation> commitFilesWithoutBlobLengths = randomCommitFiles(false);

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            StatelessCompoundCommit.Writer writer = new StatelessCompoundCommit.Writer(
                testInstance.shardId(),
                testInstance.generation(),
                testInstance.primaryTerm(),
                0,
                testInstance.nodeEphemeralId()
            );
            for (Map.Entry<String, BlobLocation> entry : commitFilesWithoutBlobLengths.entrySet()) {
                writer.addReferencedBlobFile(entry.getKey(), entry.getValue());
            }

            PositionTrackingOutputStreamStreamOutput positionTracking = new PositionTrackingOutputStreamStreamOutput(output);
            writer.writeHeaderToStore(
                positionTracking,
                randomFrom(StatelessCompoundCommit.VERSION_WITH_COMMIT_FILES, StatelessCompoundCommit.VERSION_WITH_BLOB_LENGTH)
            );

            var headerSize = positionTracking.position();
            // StatelessCompoundCommit.VERSION_WITH_COMMIT_FILES, StatelessCompoundCommit.VERSION_WITH_BLOB_LENGTH do not support
            // translogRecoveryVersion. So the deserialized value will always be 0
            StatelessCompoundCommit withOldBlobLengths = new StatelessCompoundCommit(
                testInstance.shardId(),
                testInstance.primaryTermAndGeneration(),
                0,
                testInstance.nodeEphemeralId(),
                commitFilesWithoutBlobLengths,
                headerSize
            );

            try (StreamInput in = output.bytes().streamInput()) {
                StatelessCompoundCommit compoundCommit = StatelessCompoundCommit.readFromStore(in);
                assertEqualInstances(withOldBlobLengths, compoundCommit);
            }
        }
    }

    public void testStoreCorruption() throws Exception {
        StatelessCompoundCommit testInstance = createTestInstance();

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            StatelessCompoundCommit.Writer writer = new StatelessCompoundCommit.Writer(
                testInstance.shardId(),
                testInstance.generation(),
                testInstance.primaryTerm(),
                testInstance.translogRecoveryStartFile(),
                testInstance.nodeEphemeralId()
            );
            for (Map.Entry<String, BlobLocation> entry : testInstance.commitFiles().entrySet()) {
                writer.addReferencedBlobFile(entry.getKey(), entry.getValue());
            }
            writer.writeHeaderToStore(new PositionTrackingOutputStreamStreamOutput(output), StatelessCompoundCommit.CURRENT_VERSION);
            // flip one byte anywhere
            byte[] bytes = BytesReference.toBytes(output.bytes());
            int i = randomIntBetween(0, bytes.length - 1);
            bytes[i] = (byte) ~bytes[i];
            try (StreamInput in = new ByteArrayStreamInput(bytes)) {
                try {
                    StatelessCompoundCommit.readFromStore(in);
                    assert false : "Should have thrown";
                } catch (IOException e) {
                    assertThat(e.getMessage(), containsString("Failed to read shard commit"));
                } catch (AssertionError e) {
                    assertThat(e.getMessage(), containsString("(offset + file) length is greater than blobLength"));
                }
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
        return randomCommitFiles(true);
    }

    private static Map<String, BlobLocation> randomCommitFiles(boolean blobLengths) {
        final int entries = randomInt(50);
        if (entries == 0) {
            return Map.of();
        }
        return IntStream.range(0, entries + 1).mapToObj(operand -> "file_" + operand).collect(Collectors.toMap(Function.identity(), s -> {
            long fileLength = randomLongBetween(100, 1000);
            long blobLength;
            long offset;
            if (blobLengths) {
                offset = randomLongBetween(0, 200);
                blobLength = randomLongBetween(10000, 20000);
            } else {
                offset = 0;
                blobLength = fileLength;
            }
            return new BlobLocation(randomLongBetween(1, 10), randomAlphaOfLength(10), blobLength, offset, fileLength);
        }));
    }
}
