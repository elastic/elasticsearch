/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.file;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A file of a Lucene {@link Directory} as the input of the Parquet reader, so that the engine reads its local Parquet files through the
 * directory of the shard, with no path and no Hadoop.
 */
public final class IndexInputFile implements InputFile {

    private final Directory directory;
    private final String name;

    public IndexInputFile(Directory directory, String name) {
        this.directory = directory;
        this.name = name;
    }

    @Override
    public long getLength() throws IOException {
        return directory.fileLength(name);
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return new IndexInputStream(directory.openInput(name, IOContext.DEFAULT));
    }

    private static final class IndexInputStream extends SeekableInputStream {
        private final IndexInput input;

        IndexInputStream(IndexInput input) {
            this.input = input;
        }

        @Override
        public long getPos() {
            return input.getFilePointer();
        }

        @Override
        public void seek(long newPos) throws IOException {
            input.seek(newPos);
        }

        @Override
        public int read() throws IOException {
            if (input.getFilePointer() >= input.length()) {
                return -1;
            }
            return input.readByte() & 0xFF;
        }

        @Override
        public int read(byte[] destination, int offset, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            final long remaining = input.length() - input.getFilePointer();
            if (remaining <= 0) {
                return -1;
            }
            final int count = (int) Math.min(length, remaining);
            input.readBytes(destination, offset, count);
            return count;
        }

        @Override
        public void readFully(byte[] destination) throws IOException {
            readFully(destination, 0, destination.length);
        }

        @Override
        public void readFully(byte[] destination, int start, int length) throws IOException {
            if (input.length() - input.getFilePointer() < length) {
                throw new EOFException(
                    "cannot read [" + length + "] bytes at [" + input.getFilePointer() + "] of [" + input.length() + "]"
                );
            }
            input.readBytes(destination, start, length);
        }

        @Override
        public int read(ByteBuffer destination) throws IOException {
            final long remaining = input.length() - input.getFilePointer();
            if (remaining <= 0) {
                return -1;
            }
            final int count = (int) Math.min(destination.remaining(), remaining);
            readInto(destination, count);
            return count;
        }

        @Override
        public void readFully(ByteBuffer destination) throws IOException {
            final int count = destination.remaining();
            if (input.length() - input.getFilePointer() < count) {
                throw new EOFException("cannot read [" + count + "] bytes at [" + input.getFilePointer() + "] of [" + input.length() + "]");
            }
            readInto(destination, count);
        }

        private void readInto(ByteBuffer destination, int count) throws IOException {
            if (destination.hasArray()) {
                input.readBytes(destination.array(), destination.arrayOffset() + destination.position(), count);
                destination.position(destination.position() + count);
            } else {
                final byte[] copy = new byte[count];
                input.readBytes(copy, 0, count);
                destination.put(copy);
            }
        }

        @Override
        public void close() throws IOException {
            input.close();
        }
    }
}
