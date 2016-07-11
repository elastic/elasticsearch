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
package org.apache.lucene.store;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;

/**
 * Similar to {@link SimpleFSDirectory} but only supports reading files from a folder and does not create the directory if it does not
 * exist. This is useful if we don't want to resurrect a folder that was just deleted before creating the {@link Directory}.
 *
 * Only supports the {@link Directory#openInput(String,IOContext)} method.
 */
public class SimpleReadOnlyFSDirectory extends BaseDirectory {

    protected final Path directory; // The underlying filesystem directory

    public SimpleReadOnlyFSDirectory(Path path) throws IOException {
        super(FSLockFactory.getDefault());
        directory = path.toRealPath();
    }

    @Override
    public String[] listAll() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String name) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long fileLength(String name) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameFile(String source, String dest) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        ensureOpen();
        Path path = directory.resolve(name);
        SeekableByteChannel channel = Files.newByteChannel(path, StandardOpenOption.READ);
        return new SimpleFSIndexInput("SimpleFSIndexInput(path=\"" + path + "\")", channel, context);
    }

    @Override
    public void close() throws IOException {
        isOpen = false;
    }

    /**
     * Copy of SimpleFSDirectory.SimpleFSIndexInput which is package-private class.
     *
     * Reads bytes with {@link SeekableByteChannel#read(ByteBuffer)}
     */
    static final class SimpleFSIndexInput extends BufferedIndexInput {
        /**
         * The maximum chunk size for reads of 16384 bytes.
         */
        private static final int CHUNK_SIZE = 16384;

        /** the channel we will read from */
        protected final SeekableByteChannel channel;
        /** is this instance a clone and hence does not own the file to close it */
        boolean isClone = false;
        /** start offset: non-zero in the slice case */
        protected final long off;
        /** end offset (start+length) */
        protected final long end;

        private ByteBuffer byteBuf; // wraps the buffer for NIO

        public SimpleFSIndexInput(String resourceDesc, SeekableByteChannel channel, IOContext context) throws IOException {
            super(resourceDesc, context);
            this.channel = channel;
            this.off = 0L;
            this.end = channel.size();
        }

        public SimpleFSIndexInput(String resourceDesc, SeekableByteChannel channel, long off, long length, int bufferSize) {
            super(resourceDesc, bufferSize);
            this.channel = channel;
            this.off = off;
            this.end = off + length;
            this.isClone = true;
        }

        @Override
        public void close() throws IOException {
            if (!isClone) {
                channel.close();
            }
        }

        @Override
        public SimpleFSIndexInput clone() {
            SimpleFSIndexInput clone = (SimpleFSIndexInput)super.clone();
            clone.isClone = true;
            return clone;
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            if (offset < 0 || length < 0 || offset + length > this.length()) {
                throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: offset=" + offset + ",length=" +
                    length + ",fileLength="  + this.length() + ": "  + this);
            }
            return new SimpleFSIndexInput(getFullSliceDescription(sliceDescription), channel, off + offset, length, getBufferSize());
        }

        @Override
        public final long length() {
            return end - off;
        }

        @Override
        protected void newBuffer(byte[] newBuffer) {
            super.newBuffer(newBuffer);
            byteBuf = ByteBuffer.wrap(newBuffer);
        }

        @Override
        protected void readInternal(byte[] b, int offset, int len) throws IOException {
            final ByteBuffer bb;

            // Determine the ByteBuffer we should use
            if (b == buffer) {
                // Use our own pre-wrapped byteBuf:
                assert byteBuf != null;
                bb = byteBuf;
                byteBuf.clear().position(offset);
            } else {
                bb = ByteBuffer.wrap(b, offset, len);
            }

            synchronized(channel) {
                long pos = getFilePointer() + off;

                if (pos + len > end) {
                    throw new EOFException("read past EOF: " + this);
                }

                try {
                    channel.position(pos);

                    int readLength = len;
                    while (readLength > 0) {
                        final int toRead = Math.min(CHUNK_SIZE, readLength);
                        bb.limit(bb.position() + toRead);
                        assert bb.remaining() == toRead;
                        final int i = channel.read(bb);
                        if (i < 0) { // be defensive here, even though we checked before hand, something could have changed
                            throw new EOFException("read past EOF: " + this + " off: " + offset + " len: " + len + " pos: " + pos +
                                " chunkLen: " + toRead + " end: " + end);
                        }
                        assert i > 0 : "SeekableByteChannel.read with non zero-length bb.remaining() must always read at least one byte " +
                            "(Channel is in blocking mode, see spec of ReadableByteChannel)";
                        pos += i;
                        readLength -= i;
                    }
                    assert readLength == 0;
                } catch (IOException ioe) {
                    throw new IOException(ioe.getMessage() + ": " + this, ioe);
                }
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
