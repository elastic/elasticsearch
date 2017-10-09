/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.translog;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.lucene.store.SimpleFSDirectory;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;

final class Checkpoint {

    final long offset;
    final int numOps;
    final long generation;
    final long minSeqNo;
    final long maxSeqNo;
    final long globalCheckpoint;
    final long minTranslogGeneration;

    private static final int INITIAL_VERSION = 1; // start with 1, just to recognize there was some magic serialization logic before
    private static final int CURRENT_VERSION = 2; // introduction of global checkpoints

    private static final String CHECKPOINT_CODEC = "ckp";

    // size of 6.0.0 checkpoint
    static final int FILE_SIZE = CodecUtil.headerLength(CHECKPOINT_CODEC)
        + Integer.BYTES  // ops
        + Long.BYTES // offset
        + Long.BYTES // generation
        + Long.BYTES // minimum sequence number, introduced in 6.0.0
        + Long.BYTES // maximum sequence number, introduced in 6.0.0
        + Long.BYTES // global checkpoint, introduced in 6.0.0
        + Long.BYTES // minimum translog generation in the translog - introduced in 6.0.0
        + CodecUtil.footerLength();

    // size of 5.0.0 checkpoint
    static final int V1_FILE_SIZE = CodecUtil.headerLength(CHECKPOINT_CODEC)
        + Integer.BYTES  // ops
        + Long.BYTES // offset
        + Long.BYTES // generation
        + CodecUtil.footerLength();

    /**
     * Create a new translog checkpoint.
     *
     * @param offset           the current offset in the translog
     * @param numOps           the current number of operations in the translog
     * @param generation       the current translog generation
     * @param minSeqNo         the current minimum sequence number of all operations in the translog
     * @param maxSeqNo         the current maximum sequence number of all operations in the translog
     * @param globalCheckpoint the last-known global checkpoint
     * @param minTranslogGeneration the minimum generation referenced by the translog at this moment.
     */
    Checkpoint(long offset, int numOps, long generation, long minSeqNo, long maxSeqNo, long globalCheckpoint, long minTranslogGeneration) {
        assert minSeqNo <= maxSeqNo : "minSeqNo [" + minSeqNo + "] is higher than maxSeqNo [" + maxSeqNo + "]";
        assert minTranslogGeneration <= generation :
            "minTranslogGen [" + minTranslogGeneration + "] is higher than generation [" + generation + "]";
        this.offset = offset;
        this.numOps = numOps;
        this.generation = generation;
        this.minSeqNo = minSeqNo;
        this.maxSeqNo = maxSeqNo;
        this.globalCheckpoint = globalCheckpoint;
        this.minTranslogGeneration = minTranslogGeneration;
    }

    private void write(DataOutput out) throws IOException {
        out.writeLong(offset);
        out.writeInt(numOps);
        out.writeLong(generation);
        out.writeLong(minSeqNo);
        out.writeLong(maxSeqNo);
        out.writeLong(globalCheckpoint);
        out.writeLong(minTranslogGeneration);
    }

    static Checkpoint emptyTranslogCheckpoint(final long offset, final long generation, final long globalCheckpoint,
                                              long minTranslogGeneration) {
        final long minSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        final long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        return new Checkpoint(offset, 0, generation, minSeqNo, maxSeqNo, globalCheckpoint, minTranslogGeneration);
    }

    static Checkpoint readCheckpointV6_0_0(final DataInput in) throws IOException {
        return new Checkpoint(in.readLong(), in.readInt(), in.readLong(), in.readLong(), in.readLong(), in.readLong(), in.readLong());
    }

    // reads a checksummed checkpoint introduced in ES 5.0.0
    static Checkpoint readCheckpointV5_0_0(final DataInput in) throws IOException {
        final long minSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        final long maxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
        final long globalCheckpoint = SequenceNumbers.UNASSIGNED_SEQ_NO;
        final long minTranslogGeneration = -1L;
        return new Checkpoint(in.readLong(), in.readInt(), in.readLong(), minSeqNo, maxSeqNo, globalCheckpoint, minTranslogGeneration);
    }

    @Override
    public String toString() {
        return "Checkpoint{" +
            "offset=" + offset +
            ", numOps=" + numOps +
            ", generation=" + generation +
            ", minSeqNo=" + minSeqNo +
            ", maxSeqNo=" + maxSeqNo +
            ", globalCheckpoint=" + globalCheckpoint +
            ", minTranslogGeneration=" + minTranslogGeneration +
            '}';
    }

    public static Checkpoint read(Path path) throws IOException {
        try (Directory dir = new SimpleFSDirectory(path.getParent())) {
            try (IndexInput indexInput = dir.openInput(path.getFileName().toString(), IOContext.DEFAULT)) {
                // We checksum the entire file before we even go and parse it. If it's corrupted we barf right here.
                CodecUtil.checksumEntireFile(indexInput);
                final int fileVersion = CodecUtil.checkHeader(indexInput, CHECKPOINT_CODEC, INITIAL_VERSION, CURRENT_VERSION);
                if (fileVersion == INITIAL_VERSION) {
                    assert indexInput.length() == V1_FILE_SIZE : indexInput.length();
                    return Checkpoint.readCheckpointV5_0_0(indexInput);
                } else {
                    assert fileVersion == CURRENT_VERSION : fileVersion;
                    assert indexInput.length() == FILE_SIZE : indexInput.length();
                    return Checkpoint.readCheckpointV6_0_0(indexInput);
                }
            }
        }
    }

    public static void write(ChannelFactory factory, Path checkpointFile, Checkpoint checkpoint, OpenOption... options) throws IOException {
        final ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream(FILE_SIZE) {
            @Override
            public synchronized byte[] toByteArray() {
                // don't clone
                return buf;
            }
        };
        final String resourceDesc = "checkpoint(path=\"" + checkpointFile + "\", gen=" + checkpoint + ")";
        try (OutputStreamIndexOutput indexOutput =
                 new OutputStreamIndexOutput(resourceDesc, checkpointFile.toString(), byteOutputStream, FILE_SIZE)) {
            CodecUtil.writeHeader(indexOutput, CHECKPOINT_CODEC, CURRENT_VERSION);
            checkpoint.write(indexOutput);
            CodecUtil.writeFooter(indexOutput);

            assert indexOutput.getFilePointer() == FILE_SIZE :
                "get you numbers straight; bytes written: " + indexOutput.getFilePointer() + ", buffer size: " + FILE_SIZE;
            assert indexOutput.getFilePointer() < 512 :
                "checkpoint files have to be smaller than 512 bytes for atomic writes; size: " + indexOutput.getFilePointer();

        }
        // now go and write to the channel, in one go.
        try (FileChannel channel = factory.open(checkpointFile, options)) {
            Channels.writeToChannel(byteOutputStream.toByteArray(), channel);
            // no need to force metadata, file size stays the same and we did the full fsync
            // when we first created the file, so the directory entry doesn't change as well
            channel.force(false);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Checkpoint that = (Checkpoint) o;

        if (offset != that.offset) return false;
        if (numOps != that.numOps) return false;
        if (generation != that.generation) return false;
        if (minSeqNo != that.minSeqNo) return false;
        if (maxSeqNo != that.maxSeqNo) return false;
        return globalCheckpoint == that.globalCheckpoint;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(offset);
        result = 31 * result + numOps;
        result = 31 * result + Long.hashCode(generation);
        result = 31 * result + Long.hashCode(minSeqNo);
        result = 31 * result + Long.hashCode(maxSeqNo);
        result = 31 * result + Long.hashCode(globalCheckpoint);
        return result;
    }

}
