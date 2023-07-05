/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.inputlatency;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.IndexModule;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;

/**
 * A DirectoryWrapper that sleeps for 50ms whenever an indexinput fills its internal
 * data buffer
 */
public class SimulatedLatencyDirectoryWrapper implements IndexModule.DirectoryWrapper {

    private final Consumer<String> delayer;

    public SimulatedLatencyDirectoryWrapper(Consumer<String> delayer) {
        this.delayer = delayer;
    }

    @Override
    public Directory wrap(Directory directory, ShardRouting shardRouting) {
        if (directory instanceof NIOFSDirectory niofsDir) {
            return new FilterDirectory(niofsDir) {
                @Override
                public IndexInput openInput(String name, IOContext context) throws IOException {
                    Path path = niofsDir.getDirectory().resolve(name);
                    FileChannel fc = FileChannel.open(path, StandardOpenOption.READ);
                    boolean success = false;
                    try {
                        final DelayingNIOFSIndexInput indexInput = new DelayingNIOFSIndexInput(
                            "NIOFSIndexInput(path=\"" + path + "\")",
                            fc,
                            context
                        );
                        success = true;
                        return indexInput;
                    } finally {
                        if (success == false) {
                            IOUtils.closeWhileHandlingException(fc);
                        }
                    }
                }
            };
        } else {
            throw new IllegalStateException("Can only wrap NIOFSDirectory");
        }
    }

    @SuppressForbidden(reason = "Copied from lucene")
    final class DelayingNIOFSIndexInput extends BufferedIndexInput {
        /** The maximum chunk size for reads of 16384 bytes. */
        private static final int CHUNK_SIZE = 16384;

        /** the file channel we will read from */
        protected final FileChannel channel;
        /** is this instance a clone and hence does not own the file to close it */
        boolean isClone = false;
        /** start offset: non-zero in the slice case */
        protected final long off;
        /** end offset (start+length) */
        protected final long end;

        DelayingNIOFSIndexInput(String resourceDesc, FileChannel fc, IOContext context) throws IOException {
            super(resourceDesc, context);
            this.channel = fc;
            this.off = 0L;
            this.end = fc.size();
        }

        DelayingNIOFSIndexInput(String resourceDesc, FileChannel fc, long off, long length, int bufferSize) {
            super(resourceDesc, bufferSize);
            this.channel = fc;
            this.off = off;
            this.end = off + length;
            this.isClone = true;
        }

        @Override
        public void close() throws IOException {
            if (isClone == false) {
                channel.close();
            }
        }

        @Override
        public DelayingNIOFSIndexInput clone() {
            DelayingNIOFSIndexInput clone = (DelayingNIOFSIndexInput) super.clone();
            clone.isClone = true;
            return clone;
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            if (offset < 0 || length < 0 || offset + length > this.length()) {
                throw new IllegalArgumentException(
                    "slice() "
                        + sliceDescription
                        + " out of bounds: offset="
                        + offset
                        + ",length="
                        + length
                        + ",fileLength="
                        + this.length()
                        + ": "
                        + this
                );
            }
            return new DelayingNIOFSIndexInput(getFullSliceDescription(sliceDescription), channel, off + offset, length, getBufferSize());
        }

        @Override
        public long length() {
            return end - off;
        }

        @Override
        protected void readInternal(ByteBuffer b) throws IOException {

            // Add a delay here to simulate pulling data from the blob store
            delayer.accept(this.toString());

            long pos = getFilePointer() + off;

            if (pos + b.remaining() > end) {
                throw new EOFException("read past EOF: " + this);
            }

            try {
                int readLength = b.remaining();
                while (readLength > 0) {
                    final int toRead = Math.min(CHUNK_SIZE, readLength);
                    b.limit(b.position() + toRead);
                    assert b.remaining() == toRead;
                    final int i = channel.read(b, pos);
                    if (i < 0) {
                        // be defensive here, even though we checked before hand, something could have changed
                        throw new EOFException("read past EOF: " + this + " buffer: " + b + " chunkLen: " + toRead + " end: " + end);
                    }
                    assert i > 0
                        : "FileChannel.read with non zero-length bb.remaining() must always read at least "
                            + "one byte (FileChannel is in blocking mode, see spec of ReadableByteChannel)";
                    pos += i;
                    readLength -= i;
                }
                assert readLength == 0;
            } catch (IOException ioe) {
                throw new IOException(ioe.getMessage() + ": " + this, ioe);
            }
        }

        @Override
        protected void seekInternal(long pos) throws IOException {
            if (pos > length()) {
                throw new EOFException("read past EOF: pos=" + pos + " vs length=" + length() + ": " + this);
            }
        }
    }

}
