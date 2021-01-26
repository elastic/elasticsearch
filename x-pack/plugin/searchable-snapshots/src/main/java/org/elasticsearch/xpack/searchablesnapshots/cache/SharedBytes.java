/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.elasticsearch.common.SuppressForbidden;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class SharedBytes {

    private static final StandardOpenOption[] OPEN_OPTIONS = new StandardOpenOption[] {
        StandardOpenOption.READ,
        StandardOpenOption.WRITE,
        StandardOpenOption.CREATE };

    final int numRegions;
    final long regionSize;

    private final FileChannel fileChannel;

    @SuppressForbidden(reason = "Use positional writes on purpose")
    SharedBytes(int numRegions, long regionSize, Path file) throws IOException {
        this.numRegions = numRegions;
        this.regionSize = regionSize;
        final long fileSize = numRegions * regionSize;
        if (fileSize > 0) {
            this.fileChannel = FileChannel.open(file, OPEN_OPTIONS);
            // write one byte at the end of the file to make sure all bytes are allocated
            fileChannel.write(ByteBuffer.allocate(1), fileSize - 1);
        } else {
            this.fileChannel = null;
        }
    }

    FileChannel getFileChannel(int sharedBytesPos) {
        assert fileChannel != null;
        // return fileChannel;
        return new FileChannel() {
            @Override
            public int read(ByteBuffer dst) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public int write(ByteBuffer src) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            @SuppressForbidden(reason = "Use positional writes on purpose")
            public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
                checkOffsets(offset, length);
                return fileChannel.write(srcs, offset, length);
            }

            @Override
            public long position() throws IOException {
                return fileChannel.position();
            }

            @Override
            public FileChannel position(long newPosition) throws IOException {
                checkOffsets(newPosition, 0);
                return fileChannel.position(newPosition);
            }

            @Override
            public long size() throws IOException {
                return fileChannel.size();
            }

            @Override
            public FileChannel truncate(long size) throws IOException {
                assert false;
                throw new UnsupportedOperationException();
            }

            @Override
            public void force(boolean metaData) throws IOException {
                fileChannel.force(metaData);
            }

            @Override
            public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
                checkOffsets(position, count);
                return fileChannel.transferTo(position, count, target);
            }

            @Override
            public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
                checkOffsets(position, count);
                return fileChannel.transferFrom(src, position, count);
            }

            @Override
            @SuppressForbidden(reason = "Use positional reads on purpose")
            public int read(ByteBuffer dst, long position) throws IOException {
                checkOffsets(position, dst.remaining());
                return fileChannel.read(dst, position);
            }

            @Override
            @SuppressForbidden(reason = "Use positional writes on purpose")
            public int write(ByteBuffer src, long position) throws IOException {
                checkOffsets(position, src.remaining());
                return fileChannel.write(src, position);
            }

            private void checkOffsets(long position, long length) {
                long pageStart = getPhysicalOffset(sharedBytesPos);
                long pageEnd = pageStart + regionSize;
                if (position < getPhysicalOffset(sharedBytesPos) || position > pageEnd || position + length > pageEnd) {
                    assert false;
                    throw new IllegalArgumentException("bad access");
                }
            }

            @Override
            public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
                assert false;
                throw new UnsupportedOperationException();
            }

            @Override
            public FileLock lock(long position, long size, boolean shared) throws IOException {
                assert false;
                throw new UnsupportedOperationException();
            }

            @Override
            public FileLock tryLock(long position, long size, boolean shared) throws IOException {
                assert false;
                throw new UnsupportedOperationException();
            }

            @Override
            protected void implCloseChannel() throws IOException {
                fileChannel.close();
            }
        };
    }

    long getPhysicalOffset(long chunkPosition) {
        return chunkPosition * regionSize;
    }
}
