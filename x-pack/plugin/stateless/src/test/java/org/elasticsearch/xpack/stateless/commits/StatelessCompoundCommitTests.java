/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.BufferedChecksumStreamOutput;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit.HOLLOW_TRANSLOG_RECOVERY_START_FILE;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomCompoundCommit;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomNodeEphemeralId;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomNonZeroPositiveLong;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomShardId;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomTimestampFieldValueRange;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class StatelessCompoundCommitTests extends AbstractWireSerializingTestCase<StatelessCompoundCommit> {

    @Override
    protected StatelessCompoundCommit createTestInstance() {
        return randomCompoundCommit();
    }

    @Override
    protected StatelessCompoundCommit mutateInstance(StatelessCompoundCommit instance) throws IOException {
        return switch (randomInt(10)) {
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
                instance.extraContent(),
                instance.timestampFieldValueRange()
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
                instance.extraContent(),
                instance.timestampFieldValueRange()
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
                    Map.of(),
                    instance.timestampFieldValueRange()
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
                        instance.extraContent(),
                        instance.timestampFieldValueRange()
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
                    Map.of(),
                    instance.timestampFieldValueRange()
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
                    Map.of(),
                    instance.timestampFieldValueRange()
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
                        instance.extraContent(),
                        instance.timestampFieldValueRange()
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
                    Map.of(),
                    instance.timestampFieldValueRange()
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
                    instance.extraContent(),
                    instance.timestampFieldValueRange()
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
                instance.extraContent(),
                instance.timestampFieldValueRange()
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
                    instance.extraContent(),
                    instance.timestampFieldValueRange()
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
                instance.extraContent(),
                instance.timestampFieldValueRange()
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
                instance.extraContent(),
                instance.timestampFieldValueRange()
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
                    extraContent,
                    instance.timestampFieldValueRange()
                );
            }
            case 10 -> {
                TimestampFieldValueRange newTimestampFieldValueRange;
                if (instance.timestampFieldValueRange() == null) {
                    newTimestampFieldValueRange = StatelessCompoundCommitTestUtils.randomTimestampFieldValueRange();
                } else if (randomBoolean()) {
                    newTimestampFieldValueRange = null;
                } else {
                    newTimestampFieldValueRange = randomValueOtherThanMany(
                        timestampFieldValueRange -> timestampFieldValueRange.equals(instance.timestampFieldValueRange()),
                        StatelessCompoundCommitTestUtils::randomTimestampFieldValueRange
                    );
                }
                yield StatelessCompoundCommit.newHollowStatelessCompoundCommit(
                    instance.shardId(),
                    instance.primaryTermAndGeneration(),
                    instance.commitFiles(),
                    instance.sizeInBytes(),
                    instance.internalFiles(),
                    instance.headerSizeInBytes(),
                    instance.internalFilesReplicatedRanges(),
                    instance.extraContent(),
                    newTimestampFieldValueRange
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
            StreamOutput positionTracking = new OutputStreamStreamOutput(output);

            Map<String, BlobLocation> referencedCommitBlobsWithoutBlobLength = StatelessCompoundCommitTestUtils.randomCommitFiles();
            List<InternalFile> internalFiles = new ArrayList<>();
            int internalFileCount = randomIntBetween(1, 10);
            for (int i = 0; i < internalFileCount; i++) {
                internalFiles.add(new InternalFile("internal_file_" + i, randomLongBetween(100, 1000)));
            }

            writeBwcHeader(
                positionTracking,
                testInstance.shardId(),
                testInstance.generation(),
                testInstance.primaryTerm(),
                testInstance.nodeEphemeralId(),
                referencedCommitBlobsWithoutBlobLength,
                internalFiles,
                randomFrom(StatelessCompoundCommit.VERSION_WITH_COMMIT_FILES, StatelessCompoundCommit.VERSION_WITH_BLOB_LENGTH)
            );

            var headerOffset = positionTracking.position();
            var totalSize = headerOffset + internalFiles.stream().mapToLong(InternalFile::length).sum();
            var expectedCommitFiles = StatelessCompoundCommit.combineCommitFiles(
                new BccBlob(
                    BccBlob.blobNameFromGeneration(testInstance.generation()),
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
            // translogRecoveryVersion or timestamp field value range, so the deserialized value will always be 0 and null, respectively.
            StatelessCompoundCommit withOldBlobLengths = new StatelessCompoundCommit(
                testInstance.shardId(),
                testInstance.primaryTermAndGeneration(),
                0,
                testInstance.nodeEphemeralId(),
                expectedCommitFiles.commitFiles(),
                totalSize,
                internalFiles.stream().map(InternalFile::name).collect(Collectors.toSet()),
                headerOffset,
                InternalFilesReplicatedRanges.EMPTY,
                Map.of(),
                null // BWC versions do not write any timestamp field value ranges
            );

            try (StreamInput in = output.bytes().streamInput()) {
                StatelessCompoundCommit compoundCommit = StatelessCompoundCommit.readFromStore(in);
                assertEqualInstances(withOldBlobLengths, compoundCommit);
            }
        }
    }

    // This method is moved from StatelessCompoundCommit since the production code only needs to write commit blobs with current version
    private static long writeBwcHeader(
        StreamOutput positionTracking,
        ShardId shardId,
        long generation,
        long primaryTerm,
        String nodeEphemeralId,
        Map<String, BlobLocation> referencedBlobFiles,
        List<InternalFile> internalFiles,
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
                testInstance.timestampFieldValueRange(),
                commitFiles,
                List.of(),
                InternalFilesReplicatedRanges.EMPTY,
                new OutputStreamStreamOutput(output),
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

    public void testTimestampFieldValueRangeXContent() throws IOException {
        final var range = randomTimestampFieldValueRange();

        // what the header of a compound commit has always written for the range, which is persisted
        final var inline = new BytesStreamOutput();
        try (var builder = new XContentBuilder(XContentType.SMILE.xContent(), inline)) {
            builder.startObject().field("min_millis", range.minMillis()).field("max_millis", range.maxMillis()).endObject();
        }
        final var shared = new BytesStreamOutput();
        try (var builder = new XContentBuilder(XContentType.SMILE.xContent(), shared)) {
            range.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        assertThat(shared.bytes(), equalTo(inline.bytes()));

        try (var parser = XContentType.SMILE.xContent().createParser(XContentParserConfiguration.EMPTY, shared.bytes().streamInput())) {
            assertThat(TimestampFieldValueRange.TIMESTAMP_FIELD_VALUE_RANGE_PARSER.parse(parser, null), equalTo(range));
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
                testInstance.timestampFieldValueRange(),
                testInstance.commitFiles(),
                List.of(),
                InternalFilesReplicatedRanges.EMPTY,
                new OutputStreamStreamOutput(output),
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
            assertThat(copy.timestampFieldValueRange(), equalTo(testInstance.timestampFieldValueRange()));
            assertThat(copy.commitFiles(), equalTo(testInstance.commitFiles()));
        }
    }

    public void testGetInternalFilesBoundaryOffsetInCurrentTermWithMixedFiles() {
        PrimaryTermAndGeneration previousGeneration = new PrimaryTermAndGeneration(4L, 4L);
        PrimaryTermAndGeneration currentGeneration = new PrimaryTermAndGeneration(5L, 5L);

        BlobLocation previousMin = new BlobLocation(new BccBlob(BccBlob.PREFIX + "4", previousGeneration), 50L, 25L);
        BlobLocation previousMax = new BlobLocation(new BccBlob(BccBlob.PREFIX + "4", previousGeneration), 400L, 25L);
        BlobLocation currentMin = new BlobLocation(new BccBlob(BccBlob.PREFIX + "5", currentGeneration), 100L, 50L);
        BlobLocation currentMax = new BlobLocation(new BccBlob(BccBlob.PREFIX + "5", currentGeneration), 300L, 50L);

        Map<String, BlobLocation> commitFiles = Map.of(
            "previousMin",
            previousMin,
            "previousMax",
            previousMax,
            "currentMin",
            currentMin,
            "inBetween",
            new BlobLocation(new BccBlob(BccBlob.PREFIX + "5", currentGeneration), 200L, 50L),
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
            Map.of(),
            randomTimestampFieldValueRange()
        );

        assertThat(commit.getMaxInternalFilesOffsetInCurrentGeneration(), equalTo(currentMax));
        assertThat(commit.getMinInternalFilesOffsetInCurrentGeneration(), equalTo(currentMin));
    }

    /// Commit headers written before xcontent are read from the store as locations in a [BccBlob], with or without a blob length.
    public void testReadBlobLocationFromStoreLegacyFormat() throws IOException {
        var blob = new BccBlob(BccBlob.blobNameFromGeneration(randomLongBetween(1, 1000)), randomLongBetween(1, 10));
        var expected = new BlobLocation(blob, randomLongBetween(0, 100), randomLongBetween(1, 1000));
        for (boolean includesBlobLength : new boolean[] { true, false }) {
            var out = new BytesStreamOutput();
            out.writeVLong(expected.primaryTerm());
            out.writeString(expected.blobName());
            if (includesBlobLength) {
                out.writeVLong(randomNonNegativeLong());
            }
            out.writeVLong(expected.offset());
            out.writeVLong(expected.fileLength());

            var read = StatelessCompoundCommit.readLegacyBlobLocationFromStore(out.bytes().streamInput(), includesBlobLength);
            assertThat(read, equalTo(expected));
            assertThat(read.blob(), instanceOf(BccBlob.class));
        }
    }

    /// The legacy format predates merge blobs: a merge blob name is rejected rather than read as a [BccBlob].
    public void testReadBlobLocationFromStoreLegacyFormatRejectsMergeBlobName() throws IOException {
        var mergeBlob = new MergeBlob(MergeBlob.blobName(randomByteArrayOfLength(StringHelper.ID_LENGTH)), randomLongBetween(1, 10));
        var out = new BytesStreamOutput();
        out.writeVLong(mergeBlob.primaryTerm());
        out.writeString(mergeBlob.blobName());
        out.writeVLong(0L);
        out.writeVLong(1L);

        var exception = expectThrows(
            IOException.class,
            () -> StatelessCompoundCommit.readLegacyBlobLocationFromStore(out.bytes().streamInput(), false)
        );
        assertThat(exception.getMessage(), containsString("malformed blob name [" + mergeBlob.blobName() + ']'));
    }

    /// Commit headers written with xcontent encoding keep the type of the referenced blobs: locations in a [BccBlob] and in a [MergeBlob]
    /// are read back as such from the store.
    public void testWriteAndReadXContentHeaderWithBccAndMergeBlobLocations() throws IOException {
        var bccLocation = new BlobLocation(
            new BccBlob(BccBlob.blobNameFromGeneration(randomLongBetween(1, 1000)), randomLongBetween(1, 10)),
            randomLongBetween(0, 100),
            randomLongBetween(1, 1000)
        );
        var mergeLocation = new BlobLocation(
            new MergeBlob(MergeBlob.blobName(randomByteArrayOfLength(StringHelper.ID_LENGTH)), randomLongBetween(1, 10)),
            randomLongBetween(0, 100),
            randomLongBetween(1, 1000)
        );
        var commitFiles = Map.of("bcc_file", bccLocation, "merge_file", mergeLocation);
        var shardId = randomShardId();
        long generation = randomNonZeroPositiveLong();
        long primaryTerm = randomNonZeroPositiveLong();
        var nodeEphemeralId = randomNodeEphemeralId();

        byte[] bytes;
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            StatelessCompoundCommit.writeXContentHeader(
                shardId,
                generation,
                primaryTerm,
                nodeEphemeralId,
                0,
                randomTimestampFieldValueRange(),
                commitFiles,
                List.of(),
                InternalFilesReplicatedRanges.EMPTY,
                new OutputStreamStreamOutput(output),
                randomBoolean(),
                List.of()
            );
            bytes = BytesReference.toBytes(output.bytes());
        }

        try (StreamInput in = new ByteArrayStreamInput(bytes)) {
            var copy = StatelessCompoundCommit.readFromStore(in);

            assertThat(copy.commitFiles(), equalTo(commitFiles));
            assertThat(copy.commitFiles().get("bcc_file").blob(), instanceOf(BccBlob.class));
            assertThat(copy.commitFiles().get("merge_file").blob(), instanceOf(MergeBlob.class));
        }
    }

    /// Locations that reference the same blob share a single [Blob] instance once read from the transport layer, across the commit files and
    /// the extra content.
    public void testReadFromTransportDeduplicatesBlobs() throws IOException {
        var blobName = BccBlob.blobNameFromGeneration(randomLongBetween(1, 1000));
        long term = randomLongBetween(1, 10);
        var otherBlobName = MergeBlob.blobName(randomByteArrayOfLength(StringHelper.ID_LENGTH));
        // equal but distinct blob instances
        var commitFiles = Map.of(
            "file_1",
            new BlobLocation(new BccBlob(blobName, term), 0, 10),
            "file_2",
            new BlobLocation(new BccBlob(blobName, term), 10, 10),
            "file_3",
            new BlobLocation(new MergeBlob(otherBlobName, term), 0, 10),
            "file_4",
            new BlobLocation(new MergeBlob(otherBlobName, term), 10, 10)
        );
        var extraContent = Map.of("extra", new BlobLocation(new BccBlob(blobName, term), 20, 10));
        // only hollow commits can have extra content
        var base = randomCompoundCommit(
            randomShardId(),
            new PrimaryTermAndGeneration(randomNonZeroPositiveLong(), randomNonZeroPositiveLong()),
            true
        );
        var commit = new StatelessCompoundCommit(
            base.shardId(),
            base.primaryTermAndGeneration(),
            base.translogRecoveryStartFile(),
            base.nodeEphemeralId(),
            commitFiles,
            base.sizeInBytes(),
            Set.of("file_1"),
            base.headerSizeInBytes(),
            base.internalFilesReplicatedRanges(),
            extraContent,
            base.timestampFieldValueRange()
        );

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            commit.writeTo(output);
            var copy = StatelessCompoundCommit.readFromTransport(output.bytes().streamInput());

            assertThat(copy.commitFiles(), equalTo(commitFiles));
            assertThat(copy.extraContent(), equalTo(extraContent));
            assertSame(copy.commitFiles().get("file_1").blob(), copy.commitFiles().get("file_2").blob());
            assertSame(copy.commitFiles().get("file_3").blob(), copy.commitFiles().get("file_4").blob());
            assertSame(copy.commitFiles().get("file_1").blob(), copy.extraContent().get("extra").blob());
        }
    }

    /// Locations that reference the same blob share a single [Blob] instance once read from the store, whatever the header format.
    public void testReadFromStoreDeduplicatesBlobs() throws IOException {
        var shardId = randomShardId();
        long generation = randomLongBetween(1, 1000);
        // the generation of the BCC containing the commit differs from the commit generation when the commit is not the last of the batch
        long bccGeneration = generation + randomLongBetween(1, 10);
        long term = randomLongBetween(1, 10);
        var blobName = BccBlob.blobNameFromGeneration(randomLongBetween(1, 1000));
        var containingBlobName = BccBlob.blobNameFromGeneration(bccGeneration);
        // equal but distinct blob instances, including some that reference the blob containing the commit itself
        var commitFiles = Map.of(
            "file_1",
            new BlobLocation(new BccBlob(blobName, term), 0, 10),
            "file_2",
            new BlobLocation(new BccBlob(blobName, term), 10, 10),
            "file_3",
            new BlobLocation(new BccBlob(blobName, term), 20, 10),
            "file_4",
            new BlobLocation(new BccBlob(containingBlobName, term), 0, 10)
        );
        var internalFiles = List.of(new InternalFile("internal_file", randomLongBetween(100, 1000)));
        var extraContent = List.of(new InternalFile("extra_file", randomLongBetween(100, 1000)));

        var versions = new int[] {
            StatelessCompoundCommit.VERSION_WITH_COMMIT_FILES,
            StatelessCompoundCommit.VERSION_WITH_BLOB_LENGTH,
            StatelessCompoundCommit.VERSION_WITH_XCONTENT_ENCODING };
        for (int version : versions) {
            try (BytesStreamOutput output = new BytesStreamOutput()) {
                if (version == StatelessCompoundCommit.VERSION_WITH_XCONTENT_ENCODING) {
                    StatelessCompoundCommit.writeXContentHeader(
                        shardId,
                        generation,
                        term,
                        // only hollow commits can have extra content
                        "",
                        HOLLOW_TRANSLOG_RECOVERY_START_FILE,
                        randomTimestampFieldValueRange(),
                        commitFiles,
                        internalFiles,
                        InternalFilesReplicatedRanges.EMPTY,
                        new OutputStreamStreamOutput(output),
                        randomBoolean(),
                        extraContent
                    );
                } else {
                    writeBwcHeader(
                        new OutputStreamStreamOutput(output),
                        shardId,
                        generation,
                        term,
                        randomNodeEphemeralId(),
                        commitFiles,
                        internalFiles,
                        version
                    );
                }

                try (StreamInput in = output.bytes().streamInput()) {
                    var copy = StatelessCompoundCommit.readFromStoreAtOffset(in, 0, commitGeneration -> {
                        assertThat(commitGeneration, equalTo(generation));
                        return bccGeneration;
                    });

                    for (var entry : commitFiles.entrySet()) {
                        assertThat(copy.commitFiles().get(entry.getKey()), equalTo(entry.getValue()));
                    }
                    assertSame(copy.commitFiles().get("file_1").blob(), copy.commitFiles().get("file_2").blob());
                    assertSame(copy.commitFiles().get("file_1").blob(), copy.commitFiles().get("file_3").blob());
                    // the files uploaded by the commit share the instance of the referenced locations to the same blob
                    assertSame(copy.commitFiles().get("file_4").blob(), copy.commitFiles().get("internal_file").blob());
                    if (version == StatelessCompoundCommit.VERSION_WITH_XCONTENT_ENCODING) {
                        assertSame(copy.commitFiles().get("file_4").blob(), copy.extraContent().get("extra_file").blob());
                    }
                }
            }
        }
    }
}
