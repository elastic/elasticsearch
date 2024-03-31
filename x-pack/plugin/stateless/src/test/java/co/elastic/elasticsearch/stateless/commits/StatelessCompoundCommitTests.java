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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.PositionTrackingOutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.BufferedChecksumStreamOutput;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;

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

    public void testStoreVersionCompatibility() throws Exception {
        StatelessCompoundCommit testInstance = createTestInstance();
        Map<String, BlobLocation> commitFilesWithoutBlobLengths = randomCommitFiles(false);

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            PositionTrackingOutputStreamStreamOutput positionTracking = new PositionTrackingOutputStreamStreamOutput(output);
            writeBwcHeader(
                positionTracking,
                testInstance.shardId(),
                testInstance.generation(),
                testInstance.primaryTerm(),
                testInstance.nodeEphemeralId(),
                0,
                commitFilesWithoutBlobLengths,
                List.of(),
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

    // This method is moved from StatelessCompoundCommit since the production code only needs to write commit blobs with current version
    private static long writeBwcHeader(
        PositionTrackingOutputStreamStreamOutput positionTracking,
        ShardId shardId,
        long generation,
        long primaryTerm,
        String nodeEphemeralId,
        long translogRecoveryStartFile,
        Map<String, BlobLocation> referencedBlobFiles,
        List<StatelessCompoundCommit.InternalFile> internalFiles,
        int version
    ) throws IOException {
        assert version < StatelessCompoundCommit.VERSION_WITH_XCONTENT_ENCODING;
        BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(positionTracking);
        CodecUtil.writeHeader(new OutputStreamDataOutput(out), StatelessCompoundCommit.SHARD_COMMIT_CODEC, version);
        TransportVersion.writeVersion(TransportVersion.current(), out);
        out.writeWriteable(shardId);
        out.writeVLong(generation);
        out.writeVLong(primaryTerm);
        out.writeString(nodeEphemeralId);
        out.writeMap(referencedBlobFiles, StreamOutput::writeString, (so, v) -> {
            final boolean includeBlobLength = version >= StatelessCompoundCommit.VERSION_WITH_BLOB_LENGTH;
            so.writeVLong(v.primaryTerm());
            so.writeString(v.blobName());
            if (includeBlobLength) {
                so.writeVLong(v.offset() + v.fileLength());
            }
            so.writeVLong(v.offset());
            so.writeVLong(v.fileLength());
        });
        out.writeCollection(internalFiles);
        out.flush();
        // Add 8 bytes for the header size field and 4 bytes for the checksum
        var headerSize = positionTracking.position() + 8 + 4;
        out.writeLong(headerSize);
        out.writeInt((int) out.getChecksum());
        out.flush();
        return headerSize;
    }

    public void testStoreCorruption() throws Exception {
        StatelessCompoundCommit testInstance = createTestInstance();

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            Map<String, BlobLocation> commitFiles = testInstance.commitFiles();

            StatelessCompoundCommit.writeXContentHeader(
                testInstance.shardId(),
                testInstance.generation(),
                testInstance.primaryTerm(),
                testInstance.nodeEphemeralId(),
                0,
                commitFiles,
                List.of(),
                StatelessCompoundCommit.CURRENT_VERSION,
                new PositionTrackingOutputStreamStreamOutput(output)
            );
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
