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

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;

/**
 */
class Checkpoint {

   static final int BUFFER_SIZE = Integer.BYTES  // ops
            + Long.BYTES // offset
            + Long.BYTES;// generation
    final long offset;
    final int numOps;
    final long generation;

    Checkpoint(long offset, int numOps, long generation) {
        this.offset = offset;
        this.numOps = numOps;
        this.generation = generation;
    }

    Checkpoint(DataInput in) throws IOException {
        offset = in.readLong();
        numOps = in.readInt();
        generation = in.readLong();
    }

    private void write(FileChannel channel) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);
        write(out);
        Channels.writeToChannel(buffer, channel);
    }

    private void write(DataOutput out) throws IOException {
        out.writeLong(offset);
        out.writeInt(numOps);
        out.writeLong(generation);
    }

    @Override
    public String toString() {
        return "Checkpoint{" +
                "offset=" + offset +
                ", numOps=" + numOps +
                ", translogFileGeneration= " + generation +
                '}';
    }

    public static Checkpoint read(Path path) throws IOException {
        try (InputStream in = Files.newInputStream(path)) {
            return new Checkpoint(new InputStreamDataInput(in));
        }
    }

    public static void write(Path checkpointFile, Checkpoint checkpoint, OpenOption... options) throws IOException {
        try (FileChannel channel = FileChannel.open(checkpointFile, options)) {
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
        return generation == that.generation;

    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(offset);
        result = 31 * result + numOps;
        result = 31 * result + Long.hashCode(generation);
        return result;
    }
}
