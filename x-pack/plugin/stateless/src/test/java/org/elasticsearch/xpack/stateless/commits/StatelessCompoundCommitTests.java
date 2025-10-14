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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.HOLLOW_TRANSLOG_RECOVERY_START_FILE;
import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommitTestUtils.randomCompoundCommit;
import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommitTestUtils.randomNodeEphemeralId;
import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommitTestUtils.randomNonZeroPositiveLong;
import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommitTestUtils.randomShardId;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class StatelessCompoundCommitTests extends AbstractWireSerializingTestCase<StatelessCompoundCommit> {

    @Override
    protected StatelessCompoundCommit createTestInstance() {
        return randomCompoundCommit();
    }

    @Override
    protected StatelessCompoundCommit mutateInstance(StatelessCompoundCommit instance) throws IOException {
        return switch (randomInt(9)) {
            case 0 -> new StatelessCompoundCommit(
                randomValueOtherThan(instance.shardId(), StatelessCompoundCommitTestUtils::randomShardId),
                instance.primaryTermAndGeneration(),
                instance.translogRecoveryStartFile(),
                instance.nodeEphemeralId(),
                instance.commitFiles(),
                instance.sizeInBytes(),
                instance.internalFiles(),
                instance.headerSizeInBytes(),
                instance.internalFilesReplicatedRanges(),
                instance.extraContent()
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
                instance.sizeInBytes(),
                instance.internalFiles(),
                instance.headerSizeInBytes(),
                instance.internalFilesReplicatedRanges(),
                instance.extraContent()
            );
            case 2 -> instance.hollow() ?
                // unhollowed commit
                new StatelessCompoundCommit(
                    instance.shardId(),
                    instance.primaryTermAndGeneration(),
                    randomValueOtherThan(HOLLOW_TRANSLOG_RECOVERY_START_FILE, StatelessCompoundCommitTestUtils::randomNonZeroPositiveLong),
                    randomNodeEphemeralId(),
                    instance.commitFiles(),
                    instance.sizeInBytes(),
                    instance.internalFiles(),
                    instance.headerSizeInBytes(),
                    instance.internalFilesReplicatedRanges(),
                    Map.of()
                ) : randomBoolean() ?
                // hollowed commit
                    StatelessCompoundCommit.newHollowStatelessCompoundCommit(
                        instance.shardId(),
                        instance.primaryTermAndGeneration(),
                        instance.commitFiles(),
                        instance.sizeInBytes(),
                        instance.internalFiles(),
                        instance.headerSizeInBytes(),
                        instance.internalFilesReplicatedRanges(),
                        instance.extraContent()
                    ) :
                // different unhollowed commit
                new StatelessCompoundCommit(
                    instance.shardId(),
                    instance.primaryTermAndGeneration(),
                    randomValueOtherThan(instance.translogRecoveryStartFile(), StatelessCompoundCommitTestUtils::randomNonZeroPositiveLong),
                    instance.nodeEphemeralId(),
                    instance.commitFiles(),
                    instance.sizeInBytes(),
                    instance.internalFiles(),
                    instance.headerSizeInBytes(),
                    instance.internalFilesReplicatedRanges(),
                    Map.of()
                );
            case 3 -> instance.hollow() ?
                // unhollowed commit
                new StatelessCompoundCommit(
                    instance.shardId(),
                    instance.primaryTermAndGeneration(),
                    randomValueOtherThan(HOLLOW_TRANSLOG_RECOVERY_START_FILE, StatelessCompoundCommitTestUtils::randomNonZeroPositiveLong),
                    randomNodeEphemeralId(),
                    instance.commitFiles(),
                    instance.sizeInBytes(),
                    instance.internalFiles(),
                    instance.headerSizeInBytes(),
                    instance.internalFilesReplicatedRanges(),
                    Map.of()
                ) : randomBoolean() ?
                // hollowed commit
                    StatelessCompoundCommit.newHollowStatelessCompoundCommit(
                        instance.shardId(),
                        instance.primaryTermAndGeneration(),
                        instance.commitFiles(),
                        instance.sizeInBytes(),
                        instance.internalFiles(),
                        instance.headerSizeInBytes(),
                        instance.internalFilesReplicatedRanges(),
                        instance.extraContent()
                    ) :
                // different unhollowed commit
                new StatelessCompoundCommit(
                    instance.shardId(),
                    instance.primaryTermAndGeneration(),
                    instance.translogRecoveryStartFile(),
                    randomValueOtherThan(instance.nodeEphemeralId(), StatelessCompoundCommitTestUtils::randomNodeEphemeralId),
                    instance.commitFiles(),
                    instance.sizeInBytes(),
                    instance.internalFiles(),
                    instance.headerSizeInBytes(),
                    instance.internalFilesReplicatedRanges(),
                    Map.of()
                );
            case 4 -> {
                var commitFiles = randomValueOtherThan(instance.commitFiles(), StatelessCompoundCommitTestUtils::randomCommitFiles);
                yield new StatelessCompoundCommit(
                    instance.shardId(),
                    instance.primaryTermAndGeneration(),
                    instance.translogRecoveryStartFile(),
                    instance.nodeEphemeralId(),
                    commitFiles,
                    instance.sizeInBytes(),
                    Set.copyOf(randomSubsetOf(commitFiles.keySet())),
                    instance.headerSizeInBytes(),
                    instance.internalFilesReplicatedRanges(),
                    instance.extraContent()
                );
            }
            case 5 -> new StatelessCompoundCommit(
                instance.shardId(),
                instance.primaryTermAndGeneration(),
                instance.translogRecoveryStartFile(),
                instance.nodeEphemeralId(),
                instance.commitFiles(),
                randomValueOtherThan(instance.sizeInBytes(), StatelessCompoundCommitTestUtils::randomNonZeroPositiveLong),
                instance.internalFiles(),
                instance.headerSizeInBytes(),
                instance.internalFilesReplicatedRanges(),
                instance.extraContent()
            );
            case 6 -> {
                Map<String, BlobLocation> commitFiles = instance.commitFiles().isEmpty()
                    ? randomValueOtherThan(Map.of(), StatelessCompoundCommitTestUtils::randomCommitFiles)
                    : instance.commitFiles();
                yield new StatelessCompoundCommit(
                    instance.shardId(),
                    instance.primaryTermAndGeneration(),
                    instance.translogRecoveryStartFile(),
                    instance.nodeEphemeralId(),
                    commitFiles,
                    instance.sizeInBytes(),
                    randomValueOtherThan(instance.internalFiles(), () -> Set.copyOf(randomSubsetOf(commitFiles.keySet()))),
                    instance.headerSizeInBytes(),
                    instance.internalFilesReplicatedRanges(),
                    instance.extraContent()
                );
            }
            case 7 -> new StatelessCompoundCommit(
                instance.shardId(),
                instance.primaryTermAndGeneration(),
                instance.translogRecoveryStartFile(),
                instance.nodeEphemeralId(),
                instance.commitFiles(),
                instance.sizeInBytes(),
                instance.internalFiles(),
                randomValueOtherThan(instance.headerSizeInBytes(), StatelessCompoundCommitTestUtils::randomNonZeroPositiveLong),
                instance.internalFilesReplicatedRanges(),
                instance.extraContent()
            );
            case 8 -> new StatelessCompoundCommit(
                instance.shardId(),
                instance.primaryTermAndGeneration(),
                instance.translogRecoveryStartFile(),
                instance.nodeEphemeralId(),
                instance.commitFiles(),
                instance.sizeInBytes(),
                instance.internalFiles(),
                instance.headerSizeInBytes(),
                randomValueOtherThan(
                    instance.internalFilesReplicatedRanges(),
                    StatelessCompoundCommitTestUtils::randomInternalFilesReplicatedRanges
                ),
                instance.extraContent()
            );
            case 9 -> {
                final Map<String, BlobLocation> extraContent = randomValueOtherThan(
                    instance.extraContent(),
                    () -> rarely() ? Map.of() : StatelessCompoundCommitTestUtils.randomCommitFiles()
                );
                yield StatelessCompoundCommit.newHollowStatelessCompoundCommit(
                    instance.shardId(),
                    instance.primaryTermAndGeneration(),
                    instance.commitFiles(),
                    instance.sizeInBytes(),
                    instance.internalFiles(),
                    instance.headerSizeInBytes(),
                    instance.internalFilesReplicatedRanges(),
                    extraContent
                );
            }
            default -> throw new AssertionError("Unexpected value");
        };
    }

    @Override
    protected Writeable.Reader<StatelessCompoundCommit> instanceReader() {
        return StatelessCompoundCommit::readFromTransport;
    }

    public void testStoreVersionCompatibility() throws Exception {
        StatelessCompoundCommit testInstance = randomCompoundCommit(
            randomShardId(),
            new PrimaryTermAndGeneration(randomNonZeroPositiveLong(), randomNonZeroPositiveLong()),
            // hollow shards were not supported at the time
            false
        );

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            PositionTrackingOutputStreamStreamOutput positionTracking = new PositionTrackingOutputStreamStreamOutput(output);

            Map<String, BlobLocation> referencedCommitBlobsWithoutBlobLength = StatelessCompoundCommitTestUtils.randomCommitFiles();
            List<StatelessCompoundCommit.InternalFile> internalFiles = new ArrayList<>();
            int internalFileCount = randomIntBetween(1, 10);
            for (int i = 0; i < internalFileCount; i++) {
                internalFiles.add(new StatelessCompoundCommit.InternalFile("internal_file_" + i, randomLongBetween(100, 1000)));
            }

            writeBwcHeader(
                positionTracking,
                testInstance.shardId(),
                testInstance.generation(),
                testInstance.primaryTerm(),
                testInstance.nodeEphemeralId(),
                0,
                referencedCommitBlobsWithoutBlobLength,
                internalFiles,
                randomFrom(StatelessCompoundCommit.VERSION_WITH_COMMIT_FILES, StatelessCompoundCommit.VERSION_WITH_BLOB_LENGTH)
            );

            var headerOffset = positionTracking.position();
            var totalSize = headerOffset + internalFiles.stream().mapToLong(StatelessCompoundCommit.InternalFile::length).sum();
            var expectedCommitFiles = StatelessCompoundCommit.combineCommitFiles(
                new BlobFile(
                    StatelessCompoundCommit.blobNameFromGeneration(testInstance.generation()),
                    new PrimaryTermAndGeneration(testInstance.primaryTerm(), testInstance.generation())
                ),
                InternalFilesReplicatedRanges.EMPTY,
                internalFiles,
                referencedCommitBlobsWithoutBlobLength,
                0,
                headerOffset,
                List.of()
            );
            // StatelessCompoundCommit.VERSION_WITH_COMMIT_FILES, StatelessCompoundCommit.VERSION_WITH_BLOB_LENGTH do not support
            // translogRecoveryVersion. So the deserialized value will always be 0
            StatelessCompoundCommit withOldBlobLengths = new StatelessCompoundCommit(
                testInstance.shardId(),
                testInstance.primaryTermAndGeneration(),
                0,
                testInstance.nodeEphemeralId(),
                expectedCommitFiles.commitFiles(),
                totalSize,
                internalFiles.stream().map(StatelessCompoundCommit.InternalFile::name).collect(Collectors.toSet()),
                headerOffset,
                InternalFilesReplicatedRanges.EMPTY,
                Map.of()
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
                InternalFilesReplicatedRanges.EMPTY,
                StatelessCompoundCommit.CURRENT_VERSION,
                new PositionTrackingOutputStreamStreamOutput(output),
                randomBoolean(),
                List.of()
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

    public void testShouldReadHeaderRegardlessFeatureFlagState() throws IOException {
        StatelessCompoundCommit testInstance = createTestInstance();
        var writerFeatureFlag = randomBoolean();

        byte[] bytes;

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            StatelessCompoundCommit.writeXContentHeader(
                testInstance.shardId(),
                testInstance.generation(),
                testInstance.primaryTerm(),
                testInstance.nodeEphemeralId(),
                testInstance.translogRecoveryStartFile(),
                testInstance.commitFiles(),
                List.of(),
                InternalFilesReplicatedRanges.EMPTY,
                StatelessCompoundCommit.CURRENT_VERSION,
                new PositionTrackingOutputStreamStreamOutput(output),
                writerFeatureFlag,
                List.of()
            );
            bytes = BytesReference.toBytes(output.bytes());
        }

        try (StreamInput in = new ByteArrayStreamInput(bytes)) {
            var copy = StatelessCompoundCommit.readFromStore(in);

            assertThat(copy.shardId(), equalTo(testInstance.shardId()));
            assertThat(copy.generation(), equalTo(testInstance.generation()));
            assertThat(copy.primaryTerm(), equalTo(testInstance.primaryTerm()));
            assertThat(copy.nodeEphemeralId(), equalTo(testInstance.nodeEphemeralId()));
            assertThat(copy.translogRecoveryStartFile(), equalTo(testInstance.translogRecoveryStartFile()));
            assertThat(copy.commitFiles(), equalTo(testInstance.commitFiles()));
        }
    }

    public void testGetInternalFilesBoundaryOffsetInCurrentTermWithMixedFiles() {
        PrimaryTermAndGeneration previosGeneration = new PrimaryTermAndGeneration(4L, 4L);
        PrimaryTermAndGeneration currentGeneration = new PrimaryTermAndGeneration(5L, 5L);

        BlobLocation previousMin = new BlobLocation(new BlobFile(StatelessCompoundCommit.PREFIX + "4", previosGeneration), 50L, 25L);
        BlobLocation previousMax = new BlobLocation(new BlobFile(StatelessCompoundCommit.PREFIX + "4", previosGeneration), 400L, 25L);
        BlobLocation currentMin = new BlobLocation(new BlobFile(StatelessCompoundCommit.PREFIX + "5", currentGeneration), 100L, 50L);
        BlobLocation currentMax = new BlobLocation(new BlobFile(StatelessCompoundCommit.PREFIX + "5", currentGeneration), 300L, 50L);

        Map<String, BlobLocation> commitFiles = Map.of(
            "previousMin",
            previousMin,
            "previousMax",
            previousMax,
            "currentMin",
            currentMin,
            "inBetween",
            new BlobLocation(new BlobFile(StatelessCompoundCommit.PREFIX + "5", currentGeneration), 200L, 50L),
            "currentMax",
            currentMax
        );

        StatelessCompoundCommit commit = new StatelessCompoundCommit(
            randomShardId(),
            currentGeneration,
            1L,
            "node-1",
            commitFiles,
            700L,
            Set.of("currentMin", "inBetween", "currentMax"),
            50L,
            InternalFilesReplicatedRanges.EMPTY,
            Map.of()
        );

        assertThat(commit.getMaxInternalFilesOffsetInCurrentGeneration(), equalTo(currentMax));
        assertThat(commit.getMinInternalFilesOffsetInCurrentGeneration(), equalTo(currentMin));
    }

}
