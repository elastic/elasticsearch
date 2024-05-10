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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public record TranslogMetadata(
    long offset,
    long size,
    long minSeqNo,
    long maxSeqNo,
    long totalOps,
    long shardTranslogGeneration,
    Directory directory
) implements Writeable {

    public static TranslogMetadata readFromStore(StreamInput streamInput, int version) throws IOException {
        return new TranslogMetadata(
            streamInput.readLong(),
            streamInput.readLong(),
            streamInput.readLong(),
            streamInput.readLong(),
            streamInput.readLong(),
            version >= CompoundTranslogHeader.VERSION_WITH_SHARD_TRANSLOG_GENERATION ? streamInput.readLong() : -1,
            version >= CompoundTranslogHeader.VERSION_WITH_BROKEN_DIRECTORY ? Directory.readFromStore(streamInput, version) : null
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(offset);
        out.writeLong(size);
        out.writeLong(minSeqNo);
        out.writeLong(maxSeqNo);
        out.writeLong(totalOps);
        out.writeLong(shardTranslogGeneration);
        directory.writeTo(out);
    }

    @Override
    public String toString() {
        return "TranslogMetadata{"
            + "offset="
            + offset
            + ", size="
            + size
            + ", minSeqNo="
            + minSeqNo
            + ", maxSeqNo="
            + maxSeqNo
            + ", totalOps="
            + totalOps
            + ", shardTranslogGeneration="
            + shardTranslogGeneration
            + ", directory="
            + directory
            + '}';
    }

    public record Directory(long estimatedOperationsToRecover, int[] referencedTranslogFileOffsets) implements Writeable {

        public static Directory readFromStore(StreamInput streamInput, int version) throws IOException {
            assert version >= CompoundTranslogHeader.VERSION_WITH_BROKEN_DIRECTORY;
            Directory directory = new Directory(streamInput.readVLong(), streamInput.readVIntArray());
            if (version >= CompoundTranslogHeader.VERSION_WITH_FIXED_DIRECTORY) {
                return directory;
            } else {
                return null;
            }
        }

        // Currently referenced files are serialized as vInts offset from the current generation. This means that the average referenced
        // file will take 1-2 bytes. In a degenerate case with 5 min Lucene commit interval this could lead to 1500 translog files. If every
        // shard referenced every translog file this would be ~3KB for each referenced file offsets array. With a large number of write
        // shards this could take quite a bit of space (100 shards == ~300KB). This is unlikely. However, to improve it in the future we can
        // either compress these arrays (repeated arrays would be highly compressible) or move to a bitset data structure. 1500 translog
        // files takes 188 bytes to serialize as a bitset. That would be ~18KB for 100 shards.
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(estimatedOperationsToRecover);
            out.writeVIntArray(referencedTranslogFileOffsets);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Directory directory = (Directory) o;
            return estimatedOperationsToRecover == directory.estimatedOperationsToRecover
                && Arrays.equals(referencedTranslogFileOffsets, directory.referencedTranslogFileOffsets);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(estimatedOperationsToRecover);
            result = 31 * result + Arrays.hashCode(referencedTranslogFileOffsets);
            return result;
        }

        @Override
        public String toString() {
            return "Directory{"
                + "estimatedOperationsToRecover="
                + estimatedOperationsToRecover
                + ", referencedTranslogFileOffsets="
                + Arrays.toString(referencedTranslogFileOffsets)
                + '}';
        }
    }
}
