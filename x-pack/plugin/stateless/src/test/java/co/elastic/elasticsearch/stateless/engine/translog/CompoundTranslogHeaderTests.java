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
        Map<ShardId, TranslogMetadata> metadata = new HashMap<>();
        int n = randomIntBetween(5, 30);
        for (int i = 0; i < n; ++i) {
            TranslogMetadata value = new TranslogMetadata(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong()
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
            TranslogMetadata newValue = switch (randomInt(4)) {
                case 0 -> new TranslogMetadata(
                    randomValueOtherThan(oldValue.offset(), CompoundTranslogHeaderTests::randomNonNegativeLong),
                    oldValue.size(),
                    oldValue.minSeqNo(),
                    oldValue.maxSeqNo(),
                    oldValue.totalOps()
                );
                case 1 -> new TranslogMetadata(
                    oldValue.offset(),
                    randomValueOtherThan(oldValue.size(), CompoundTranslogHeaderTests::randomNonNegativeLong),
                    oldValue.minSeqNo(),
                    oldValue.maxSeqNo(),
                    oldValue.totalOps()
                );
                case 2 -> new TranslogMetadata(
                    oldValue.offset(),
                    oldValue.size(),
                    randomValueOtherThan(oldValue.minSeqNo(), CompoundTranslogHeaderTests::randomNonNegativeLong),
                    oldValue.maxSeqNo(),
                    oldValue.totalOps()
                );
                case 3 -> new TranslogMetadata(
                    oldValue.offset(),
                    oldValue.size(),
                    oldValue.minSeqNo(),
                    randomValueOtherThan(oldValue.maxSeqNo(), CompoundTranslogHeaderTests::randomNonNegativeLong),
                    oldValue.totalOps()
                );
                case 4 -> new TranslogMetadata(
                    oldValue.offset(),
                    oldValue.size(),
                    oldValue.minSeqNo(),
                    oldValue.maxSeqNo(),
                    randomValueOtherThan(oldValue.totalOps(), CompoundTranslogHeaderTests::randomNonNegativeLong)
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
                expectThrows(TranslogCorruptedException.class, () -> CompoundTranslogHeader.readFromStore("test", in));
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
        CompoundTranslogHeader testInstance = createTestInstance().compoundTranslogHeader();
        try (
            BytesStreamOutput output = new BytesStreamOutput();
            BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(output)
        ) {
            // Serialize in old version
            out.writeMap(testInstance.metadata());
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
