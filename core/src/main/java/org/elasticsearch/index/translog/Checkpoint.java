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
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.lucene.store.SimpleFSDirectory;
import org.elasticsearch.common.io.Channels;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

/**
 */
class Checkpoint {

    final long offset;
    final int numOps;
    final long generation;

    private static final int INITIAL_VERSION = 1; // start with 1, just to recognize there was some magic serialization logic before

    private static final String CHECKPOINT_CODEC = "ckp";

    static final int BUFFER_SIZE = CodecUtil.headerLength(CHECKPOINT_CODEC)
        + Integer.BYTES  // ops
        + Long.BYTES // offset
        + Long.BYTES // generation
        + CodecUtil.footerLength();

    Checkpoint(long offset, int numOps, long generation) {
        this.offset = offset;
        this.numOps = numOps;
        this.generation = generation;
    }

    private void write(DataOutput out) throws IOException {
        out.writeLong(offset);
        out.writeInt(numOps);
        out.writeLong(generation);
    }

    static Checkpoint readChecksummed(DataInput in) throws IOException {
        return new Checkpoint(in.readLong(), in.readInt(), in.readLong());
    }

    static Checkpoint readNonChecksummed(DataInput in) throws IOException {
        return new Checkpoint(in.readLong(), in.readInt(), in.readLong());
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
        try (Directory dir = new SimpleFSDirectory(path.getParent())) {
            try (final IndexInput indexInput = dir.openInput(path.getFileName().toString(), IOContext.DEFAULT)) {
                if (indexInput.length() == 20) {
                    // OLD unchecksummed file that was written < ES 5.0.0
                    return Checkpoint.readNonChecksummed(indexInput);
                }
                // We checksum the entire file before we even go and parse it. If it's corrupted we barf right here.
                CodecUtil.checksumEntireFile(indexInput);
                final int fileVersion = CodecUtil.checkHeader(indexInput, CHECKPOINT_CODEC, INITIAL_VERSION, INITIAL_VERSION);
                assert fileVersion == INITIAL_VERSION : "trust no one! " + fileVersion;
                return Checkpoint.readChecksummed(indexInput);
            }
        }
    }

    public static void write(ChannelFactory factory, Path checkpointFile, Checkpoint checkpoint, OpenOption... options) throws IOException {
        final ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream(BUFFER_SIZE) {
            @Override
            public synchronized byte[] toByteArray() {
                // don't clone
                return buf;
            }
        };
        final String resourceDesc = "checkpoint(path=\"" + checkpointFile + "\", gen=" + checkpoint + ")";
        try (final DirectOutputStreamIndexOutput indexOutput =
                 new DirectOutputStreamIndexOutput(resourceDesc, checkpointFile.toString(), byteOutputStream)) {
            CodecUtil.writeHeader(indexOutput, CHECKPOINT_CODEC, INITIAL_VERSION);
            checkpoint.write(indexOutput);
            CodecUtil.writeFooter(indexOutput);

            assert indexOutput.getFilePointer() == BUFFER_SIZE :
                "get you number straights. Bytes written: " + indexOutput.getFilePointer() + " buffer size: " + BUFFER_SIZE;
            assert indexOutput.getFilePointer() < 512 :
                "checkpoint files have to be smaller 512b for atomic writes. size: " + indexOutput.getFilePointer();

        }
        // now go and write to the channel, in one go.
        try (FileChannel channel = factory.open(checkpointFile, options)) {
            Channels.writeToChannel(byteOutputStream.toByteArray(), channel);
            // no need to force metadata, file size stays the same
            channel.force(false);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Checkpoint that = (Checkpoint) o;

        if (offset != that.offset) {
            return false;
        }
        if (numOps != that.numOps) {
            return false;
        }
        return generation == that.generation;

    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(offset);
        result = 31 * result + numOps;
        result = 31 * result + Long.hashCode(generation);
        return result;
    }

    /** A copy of {@link OutputStreamIndexOutput} without the intermediate buffering */
    static class DirectOutputStreamIndexOutput extends IndexOutput {

        private final CRC32 crc = new CRC32();
        private final CheckedOutputStream os;

        private long bytesWritten = 0L;

        /**
         * Creates a new {@link OutputStreamIndexOutput} with the given buffer size.
         *
         * @throws IllegalArgumentException if the given buffer size is less or equal to <tt>0</tt>
         */
        public DirectOutputStreamIndexOutput(String resourceDescription, String name, OutputStream out) {
            super(resourceDescription, name);
            this.os = new CheckedOutputStream(out, crc);
        }

        @Override
        public final void writeByte(byte b) throws IOException {
            os.write(b);
            bytesWritten++;
        }

        @Override
        public final void writeBytes(byte[] b, int offset, int length) throws IOException {
            os.write(b, offset, length);
            bytesWritten += length;
        }

        @Override
        public void close() throws IOException {
            os.close();
        }

        @Override
        public final long getFilePointer() {
            return bytesWritten;
        }

        @Override
        public final long getChecksum() throws IOException {
            return crc.getValue();
        }
    }
}
