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

package co.elastic.elasticsearch.stateless.engine.translog;

import junit.framework.AssertionFailedError;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.BufferedChecksumStreamOutput;
import org.elasticsearch.index.translog.TranslogCorruptedException;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static co.elastic.elasticsearch.stateless.engine.translog.CompoundTranslogHeader.PINNED_TRANSPORT_VERSION;

public class CompoundTranslogHeaderTests extends AbstractWireSerializingTestCase<
    CompoundTranslogHeaderTests.WritableCompoundTranslogHeader> {

    // CompoundTranslogHeader is not currently meant to be sent over the wire, so it is not writable (because we pin the transport version).
    // However, this test provides value so we lightly wrap it to test serialization against AbstractWireSerializingTestCase
    record WritableCompoundTranslogHeader(CompoundTranslogHeader compoundTranslogHeader) implements Writeable {

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            compoundTranslogHeader.writeToStore(out);
        }
    }

    @Override
    protected Writeable.Reader<WritableCompoundTranslogHeader> instanceReader() {
        return (in) -> new WritableCompoundTranslogHeader(CompoundTranslogHeader.readFromStore("test", in));
    }

    @Override
    protected WritableCompoundTranslogHeader createTestInstance() {
        return createTestInstance(true, true);
    }

    private static WritableCompoundTranslogHeader createTestInstance(boolean includeShardTranslogGeneration, boolean includeDirectory) {
        Map<ShardId, TranslogMetadata> metadata = new HashMap<>();
        int n = randomIntBetween(5, 30);
        for (int i = 0; i < n; ++i) {
            TranslogMetadata value = new TranslogMetadata(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                includeShardTranslogGeneration ? randomNonNegativeLong() : -1,
                includeDirectory ? new TranslogMetadata.Directory(randomNonNegativeLong(), randomInts(10).toArray()) : null
            );
            metadata.put(new ShardId(randomAlphaOfLength(10), randomAlphaOfLength(20), randomIntBetween(0, 10)), value);

        }
        CompoundTranslogHeader compoundTranslogHeader = new CompoundTranslogHeader(metadata);
        return new WritableCompoundTranslogHeader(compoundTranslogHeader);
    }

    @Override
    protected WritableCompoundTranslogHeader mutateInstance(WritableCompoundTranslogHeader writableInstance) {
        CompoundTranslogHeader instance = writableInstance.compoundTranslogHeader();
        HashMap<ShardId, TranslogMetadata> newMetadata = new HashMap<>();
        for (Map.Entry<ShardId, TranslogMetadata> metadata : instance.metadata().entrySet()) {
            TranslogMetadata oldValue = metadata.getValue();
            TranslogMetadata newValue = switch (randomInt(7)) {
                case 0 -> new TranslogMetadata(
                    randomValueOtherThan(oldValue.offset(), CompoundTranslogHeaderTests::randomNonNegativeLong),
                    oldValue.size(),
                    oldValue.minSeqNo(),
                    oldValue.maxSeqNo(),
                    oldValue.totalOps(),
                    oldValue.shardTranslogGeneration(),
                    oldValue.directory()
                );
                case 1 -> new TranslogMetadata(
                    oldValue.offset(),
                    randomValueOtherThan(oldValue.size(), CompoundTranslogHeaderTests::randomNonNegativeLong),
                    oldValue.minSeqNo(),
                    oldValue.maxSeqNo(),
                    oldValue.totalOps(),
                    oldValue.shardTranslogGeneration(),
                    oldValue.directory()
                );
                case 2 -> new TranslogMetadata(
                    oldValue.offset(),
                    oldValue.size(),
                    randomValueOtherThan(oldValue.minSeqNo(), CompoundTranslogHeaderTests::randomNonNegativeLong),
                    oldValue.maxSeqNo(),
                    oldValue.totalOps(),
                    oldValue.shardTranslogGeneration(),
                    oldValue.directory()
                );
                case 3 -> new TranslogMetadata(
                    oldValue.offset(),
                    oldValue.size(),
                    oldValue.minSeqNo(),
                    randomValueOtherThan(oldValue.maxSeqNo(), CompoundTranslogHeaderTests::randomNonNegativeLong),
                    oldValue.totalOps(),
                    oldValue.shardTranslogGeneration(),
                    oldValue.directory()
                );
                case 4 -> new TranslogMetadata(
                    oldValue.offset(),
                    oldValue.size(),
                    oldValue.minSeqNo(),
                    oldValue.maxSeqNo(),
                    randomValueOtherThan(oldValue.totalOps(), CompoundTranslogHeaderTests::randomNonNegativeLong),
                    oldValue.shardTranslogGeneration(),
                    oldValue.directory()
                );
                case 5 -> new TranslogMetadata(
                    oldValue.offset(),
                    oldValue.size(),
                    oldValue.minSeqNo(),
                    oldValue.maxSeqNo(),
                    oldValue.totalOps(),
                    randomValueOtherThan(oldValue.shardTranslogGeneration(), CompoundTranslogHeaderTests::randomNonNegativeLong),
                    oldValue.directory()
                );
                case 6 -> new TranslogMetadata(
                    oldValue.offset(),
                    oldValue.size(),
                    oldValue.minSeqNo(),
                    oldValue.maxSeqNo(),
                    oldValue.totalOps(),
                    oldValue.shardTranslogGeneration(),
                    new TranslogMetadata.Directory(
                        randomValueOtherThan(
                            oldValue.directory().estimatedOperationsToRecover(),
                            CompoundTranslogHeaderTests::randomNonNegativeLong
                        ),
                        oldValue.directory().referencedTranslogFileOffsets()
                    )
                );
                case 7 -> new TranslogMetadata(
                    oldValue.offset(),
                    oldValue.size(),
                    oldValue.minSeqNo(),
                    oldValue.maxSeqNo(),
                    oldValue.totalOps(),
                    oldValue.shardTranslogGeneration(),
                    new TranslogMetadata.Directory(
                        oldValue.directory().estimatedOperationsToRecover(),
                        randomValueOtherThan(oldValue.directory().referencedTranslogFileOffsets(), () -> randomInts(10).toArray())
                    )
                );
                default -> throw new AssertionError("Unexpected value");
            };
            newMetadata.put(metadata.getKey(), newValue);
        }
        return new WritableCompoundTranslogHeader(new CompoundTranslogHeader(newMetadata));
    }

    public void testStoreCorruption() throws Exception {
        CompoundTranslogHeader testInstance = createTestInstance().compoundTranslogHeader();

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            testInstance.writeToStore(output);
            // flip one checksum byte
            byte[] bytes = BytesReference.toBytes(output.bytes());
            int i = randomIntBetween(bytes.length - 5, bytes.length - 1);
            bytes[i] = (byte) ~bytes[i];
            try (StreamInput in = new ByteArrayStreamInput(bytes)) {
                try {
                    expectThrows(TranslogCorruptedException.class, () -> CompoundTranslogHeader.readFromStore("test", in));
                } catch (AssertionFailedError e) {
                    // These are known scenarios where a byte flip can break the serialization in other ways. Since it is not silent, it is
                    // fine.
                    if (e.getMessage().contains("but got java.io.IOException: Invalid vInt") == false
                        && e.getMessage().contains("but got java.lang.ArrayIndexOutOfBoundsException") == false) {
                        throw e;
                    }
                }
            }
        }
    }

    public void testReadOnlyCompoundHeaderStreamIsPositionedAtData() throws Exception {
        CompoundTranslogHeader testInstance = createTestInstance().compoundTranslogHeader();
        try (BytesStreamOutput output = new BytesStreamOutput();) {
            testInstance.writeToStore(output);
            long data = randomLong();
            output.writeLong(data);

            StreamInput streamInput = output.bytes().streamInput();
            CompoundTranslogHeader compoundTranslogHeader = CompoundTranslogHeader.readFromStore("test", streamInput);
            assertEquals(testInstance.metadata(), compoundTranslogHeader.metadata());
            // Test that the stream is at the correct place to read follow-up data
            assertEquals(data, streamInput.readLong());
        }
    }

    public void testReadPreCodecVersion() throws Exception {
        CompoundTranslogHeader testInstance = createTestInstance(false, false).compoundTranslogHeader();
        try (
            BytesStreamOutput output = new BytesStreamOutput();
            BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(output)
        ) {
            // Serialize in old version
            out.writeMap(testInstance.metadata(), StreamOutput::writeWriteable, (out1, value) -> {
                out1.writeLong(value.offset());
                out1.writeLong(value.size());
                out1.writeLong(value.minSeqNo());
                out1.writeLong(value.maxSeqNo());
                out1.writeLong(value.totalOps());
            });
            out.writeLong(out.getChecksum());
            long data = randomLong();
            out.writeLong(data);
            out.flush();

            expectThrows(
                CompoundTranslogHeader.NoVersionCodecException.class,
                () -> CompoundTranslogHeader.readFromStore("test", output.bytes().streamInput())
            );
            StreamInput oldStreamInput = output.bytes().streamInput();
            CompoundTranslogHeader compoundTranslogHeader = CompoundTranslogHeader.readFromStoreOld("test", oldStreamInput);
            assertEquals(testInstance.metadata(), compoundTranslogHeader.metadata());
            // Test that the stream is at the correct place to read follow-up data
            assertEquals(data, oldStreamInput.readLong());
        }
    }

    public void testReadPreShardGenerationVersion() throws Exception {
        CompoundTranslogHeader testInstance = createTestInstance(false, false).compoundTranslogHeader();
        try (
            BytesStreamOutput output = new BytesStreamOutput();
            BufferedChecksumStreamOutput streamOutput = new BufferedChecksumStreamOutput(output)
        ) {
            streamOutput.setTransportVersion(PINNED_TRANSPORT_VERSION);
            BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(streamOutput);
            CodecUtil.writeHeader(
                new OutputStreamDataOutput(out),
                CompoundTranslogHeader.TRANSLOG_REPLICATOR_CODEC,
                CompoundTranslogHeader.VERSION_WITH_TRANSPORT_VERSION
            );
            // Serialize in old version
            out.writeMap(testInstance.metadata(), StreamOutput::writeWriteable, (out1, value) -> {
                out1.writeLong(value.offset());
                out1.writeLong(value.size());
                out1.writeLong(value.minSeqNo());
                out1.writeLong(value.maxSeqNo());
                out1.writeLong(value.totalOps());
            });
            out.writeInt((int) out.getChecksum());
            long data = randomLong();
            out.writeLong(data);
            out.flush();

            StreamInput oldStreamInput = output.bytes().streamInput();
            CompoundTranslogHeader compoundTranslogHeader = CompoundTranslogHeader.readFromStore("test", oldStreamInput);
            assertEquals(testInstance.metadata(), compoundTranslogHeader.metadata());
            // Test that the stream is at the correct place to read follow-up data
            assertEquals(data, oldStreamInput.readLong());
        }
    }

    public void testReadPreDirectoryVersion() throws Exception {
        CompoundTranslogHeader testInstance = createTestInstance(false, false).compoundTranslogHeader();
        try (
            BytesStreamOutput output = new BytesStreamOutput();
            BufferedChecksumStreamOutput streamOutput = new BufferedChecksumStreamOutput(output)
        ) {
            streamOutput.setTransportVersion(PINNED_TRANSPORT_VERSION);
            BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(streamOutput);
            CodecUtil.writeHeader(
                new OutputStreamDataOutput(out),
                CompoundTranslogHeader.TRANSLOG_REPLICATOR_CODEC,
                CompoundTranslogHeader.VERSION_WITH_SHARD_TRANSLOG_GENERATION
            );
            // Serialize in old version
            out.writeMap(testInstance.metadata(), StreamOutput::writeWriteable, (out1, value) -> {
                out1.writeLong(value.offset());
                out1.writeLong(value.size());
                out1.writeLong(value.minSeqNo());
                out1.writeLong(value.maxSeqNo());
                out1.writeLong(value.totalOps());
                out1.writeLong(value.shardTranslogGeneration());
            });
            out.writeInt((int) out.getChecksum());
            long data = randomLong();
            out.writeLong(data);
            out.flush();

            StreamInput oldStreamInput = output.bytes().streamInput();
            CompoundTranslogHeader compoundTranslogHeader = CompoundTranslogHeader.readFromStore("test", oldStreamInput);
            assertEquals(testInstance.metadata(), compoundTranslogHeader.metadata());
            // Test that the stream is at the correct place to read follow-up data
            assertEquals(data, oldStreamInput.readLong());
        }
    }

    public void testReadBrokenDirectoryVersion() throws Exception {
        CompoundTranslogHeader testInstance = createTestInstance(false, false).compoundTranslogHeader();
        try (
            BytesStreamOutput output = new BytesStreamOutput();
            BufferedChecksumStreamOutput streamOutput = new BufferedChecksumStreamOutput(output)
        ) {
            streamOutput.setTransportVersion(PINNED_TRANSPORT_VERSION);
            BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(streamOutput);
            CodecUtil.writeHeader(
                new OutputStreamDataOutput(out),
                CompoundTranslogHeader.TRANSLOG_REPLICATOR_CODEC,
                CompoundTranslogHeader.VERSION_WITH_BROKEN_DIRECTORY
            );
            // Serialize in old version
            out.writeMap(testInstance.metadata(), StreamOutput::writeWriteable, (out1, value) -> {
                out1.writeLong(value.offset());
                out1.writeLong(value.size());
                out1.writeLong(value.minSeqNo());
                out1.writeLong(value.maxSeqNo());
                out1.writeLong(value.totalOps());
                out1.writeLong(value.shardTranslogGeneration());
                // Serialize a random directory to test it will not be read on deserialization
                new TranslogMetadata.Directory(10, new int[] { 10, 8, 4 }).writeTo(out1);
            });

            out.writeInt((int) out.getChecksum());
            long data = randomLong();
            out.writeLong(data);
            out.flush();

            StreamInput oldStreamInput = output.bytes().streamInput();
            CompoundTranslogHeader compoundTranslogHeader = CompoundTranslogHeader.readFromStore("test", oldStreamInput);
            assertEquals(testInstance.metadata(), compoundTranslogHeader.metadata());
            // Test that the stream is at the correct place to read follow-up data
            assertEquals(data, oldStreamInput.readLong());
        }
    }

    public void testStreamVersionPinned() throws Exception {
        CompoundTranslogHeader testInstance = createTestInstance().compoundTranslogHeader();
        try (BytesStreamOutput output = new BytesStreamOutput();) {
            testInstance.writeToStore(output);
            assertEquals(PINNED_TRANSPORT_VERSION, output.getTransportVersion());

            try (StreamInput input = output.bytes().streamInput()) {
                CompoundTranslogHeader.readFromStore("test", input);
                assertEquals(PINNED_TRANSPORT_VERSION, output.getTransportVersion());
            }
        }
    }
}
