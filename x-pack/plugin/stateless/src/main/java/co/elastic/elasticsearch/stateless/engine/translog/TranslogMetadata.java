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

public record TranslogMetadata(long offset, long size, Operations operations, Directory directory) implements Writeable {

    public static TranslogMetadata readFromStore(StreamInput streamInput, int version) throws IOException {
        long offset = streamInput.readLong();
        long size = streamInput.readLong();
        Operations operations = Operations.readFromStore(streamInput);
        if (version < CompoundTranslogHeader.VERSION_WITH_REMOVED_SHARD_GENERATION_DIRECTORY) {
            streamInput.readLong();
        }
        return new TranslogMetadata(offset, size, operations, Directory.readFromStore(streamInput));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(offset);
        out.writeLong(size);
        operations.writeTo(out);
        directory.writeTo(out);
    }

    public record Operations(long minSeqNo, long maxSeqNo, long totalOps) implements Writeable {

        public static Operations readFromStore(StreamInput streamInput) throws IOException {
            long minSeqNo = streamInput.readLong();
            long maxSeqNo = streamInput.readLong();
            long totalOps = streamInput.readLong();
            return new Operations(minSeqNo, maxSeqNo, totalOps);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(minSeqNo);
            out.writeLong(maxSeqNo);
            out.writeLong(totalOps);
        }
    }

    public record Directory(long estimatedOperationsToRecover, int[] referencedTranslogFileOffsets) implements Writeable {

        public static Directory readFromStore(StreamInput streamInput) throws IOException {
            return new Directory(streamInput.readVLong(), streamInput.readVIntArray());
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
            return "Directory["
                + "estimatedOperationsToRecover="
                + estimatedOperationsToRecover
                + ", referencedTranslogFileOffsets="
                + Arrays.toString(referencedTranslogFileOffsets)
                + ']';
        }
    }
}
