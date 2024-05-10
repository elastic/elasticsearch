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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.BufferedChecksumStreamInput;
import org.elasticsearch.index.translog.BufferedChecksumStreamOutput;
import org.elasticsearch.index.translog.TranslogCorruptedException;

import java.io.IOException;
import java.util.Map;

public record CompoundTranslogHeader(Map<ShardId, TranslogMetadata> metadata) {

    static final String TRANSLOG_REPLICATOR_CODEC = "translog_replicator_file";
    // Pin the transport version to 8.9 to ensure that serialization changes of used types can be read without version negotiation. In the
    // future this might need to be advanced if 8.9 is no longer available.
    static final TransportVersion PINNED_TRANSPORT_VERSION = TransportVersion.fromId(8_09_00_99);
    static final int VERSION_WITH_TRANSPORT_VERSION = 0;
    static final int VERSION_WITH_SHARD_TRANSLOG_GENERATION = 1;
    static final int VERSION_WITH_BROKEN_DIRECTORY = 2;
    static final int VERSION_WITH_FIXED_DIRECTORY = 3;
    private static final int CURRENT_VERSION = VERSION_WITH_FIXED_DIRECTORY;

    public static CompoundTranslogHeader readFromStore(String name, StreamInput streamInput) throws IOException {
        streamInput.setTransportVersion(PINNED_TRANSPORT_VERSION);
        BufferedChecksumStreamInput input = new BufferedChecksumStreamInput(streamInput, TRANSLOG_REPLICATOR_CODEC);
        InputStreamDataInput dataInput = new InputStreamDataInput(input);
        int magic = CodecUtil.readBEInt(dataInput);
        if (magic == CodecUtil.CODEC_MAGIC) {
            int version = CodecUtil.checkHeaderNoMagic(
                dataInput,
                TRANSLOG_REPLICATOR_CODEC,
                VERSION_WITH_TRANSPORT_VERSION,
                CURRENT_VERSION
            );
            return readHeader(name, input, version);
        } else {
            throw new NoVersionCodecException();
        }
    }

    public static CompoundTranslogHeader readFromStoreOld(String name, StreamInput streamInput) throws IOException {
        BufferedChecksumStreamInput input = new BufferedChecksumStreamInput(streamInput, TRANSLOG_REPLICATOR_CODEC);
        Map<ShardId, TranslogMetadata> metadata = input.readMap(ShardId::new, (in) -> TranslogMetadata.readFromStore(in, 0));
        // Verify checksum of compound file
        long expectedChecksum = input.getChecksum();
        long readChecksum = input.readLong();
        if (readChecksum != expectedChecksum) {
            throw new TranslogCorruptedException(
                name,
                "checksum verification failed - expected: 0x"
                    + Long.toHexString(expectedChecksum)
                    + ", got: 0x"
                    + Long.toHexString(readChecksum)
            );
        }
        return new CompoundTranslogHeader(metadata);
    }

    private static CompoundTranslogHeader readHeader(String name, BufferedChecksumStreamInput input, int version) throws IOException {
        Map<ShardId, TranslogMetadata> metadata = input.readMap(ShardId::new, (in) -> TranslogMetadata.readFromStore(in, version));
        // Verify checksum of compound file
        long expectedChecksum = input.getChecksum();
        long readChecksum = Integer.toUnsignedLong(input.readInt());
        if (readChecksum != expectedChecksum) {
            throw new TranslogCorruptedException(
                name,
                "checksum verification failed - expected: 0x"
                    + Long.toHexString(expectedChecksum)
                    + ", got: 0x"
                    + Long.toHexString(readChecksum)
            );
        }
        return new CompoundTranslogHeader(metadata);
    }

    public void writeToStore(StreamOutput streamOutput) throws IOException {
        streamOutput.setTransportVersion(PINNED_TRANSPORT_VERSION);
        BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(streamOutput);
        CodecUtil.writeHeader(new OutputStreamDataOutput(out), TRANSLOG_REPLICATOR_CODEC, CURRENT_VERSION);
        out.writeMap(metadata);
        out.writeInt((int) out.getChecksum());
        out.flush();
    }

    public static class NoVersionCodecException extends ElasticsearchException {
        public NoVersionCodecException() {
            super("Comes from a translog written without versioning");
        }
    }
}
