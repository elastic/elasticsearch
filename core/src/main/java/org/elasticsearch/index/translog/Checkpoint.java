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

import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.InputStreamDataInput;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.index.seqno.SequenceNumbersService;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;

/**
 */
public class Checkpoint {

    static final byte FORMAT_VERSION = 1;

   static final int BUFFER_SIZE = Integer.BYTES  // ops
            + Long.BYTES // offset
            + Long.BYTES // generation
            + Long.BYTES; // seq no global checkpoint
    final long offset;
    final int numOps;
    final long generation;
    final long seqNoGlobalCheckpoint;

    Checkpoint(long offset, int numOps, long generation, long seqNoGlobalCheckpoint) {
        if ((offset & (0xFFL << 56)) != 0L) {
            throw new IllegalArgumentException("offset should always leave space for a version byte ");
        }
        this.offset = offset;
        this.numOps = numOps;
        this.generation = generation;
        this.seqNoGlobalCheckpoint = seqNoGlobalCheckpoint;
    }

    Checkpoint(DataInput in) throws IOException {
        long longOffestAndVersion = in.readLong();
        byte version = (byte) (longOffestAndVersion >> 56);
        offset = longOffestAndVersion & 0x00FFFFFFFFFFFFFFL;
        numOps = in.readInt();
        generation = in.readLong();
        if (version >= 1) {
            seqNoGlobalCheckpoint = in.readLong();
        } else {
            seqNoGlobalCheckpoint = SequenceNumbersService.UNASSIGNED_SEQ_NO;
        }
    }

    private void write(FileChannel channel) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        write(out);
        Channels.writeToChannel(buffer, channel);
    }

    private void write(DataOutput out) throws IOException {
        out.writeLong(offset | ((long)FORMAT_VERSION << 56));
        out.writeInt(numOps);
        out.writeLong(generation);
        out.writeLong(seqNoGlobalCheckpoint);
    }

    @Override
    public String toString() {
        return "Checkpoint{" +
                "offset=" + offset +
                ", numOps=" + numOps +
                ", translogFileGeneration= " + generation +
                ", seqNoGlobalCheckpoint= " + seqNoGlobalCheckpoint +
                '}';
    }

    public long getOffset() {
        return offset;
    }

    public int getNumOps() {
        return numOps;
    }

    public long getGeneration() {
        return generation;
    }

    public long getSeqNoGlobalCheckpoint() {
        return seqNoGlobalCheckpoint;
    }

    public static Checkpoint read(Path path) throws IOException {
        try (InputStream in = Files.newInputStream(path)) {
            return new Checkpoint(new InputStreamDataInput(in));
        }
    }

    public static void write(ChannelFactory factory, Path checkpointFile, Checkpoint checkpoint, OpenOption... options) throws IOException {
        try (FileChannel channel = factory.open(checkpointFile, options)) {
            checkpoint.write(channel);
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
        if (seqNoGlobalCheckpoint != that.seqNoGlobalCheckpoint) return false;
        return generation == that.generation;

    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(offset);
        result = 31 * result + numOps;
        result = 31 * result + Long.hashCode(generation);
        result = 31 * result + Long.hashCode(seqNoGlobalCheckpoint);
        return result;
    }
}
