/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.Channels;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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

    SharedBytes(int numRegions, long regionSize, Path file) throws IOException {
        this.numRegions = numRegions;
        this.regionSize = regionSize;
        final long fileSize = numRegions * regionSize;
        if (fileSize > 0) {
            final ByteBuffer fillBytes = ByteBuffer.allocate(Channels.WRITE_CHUNK_SIZE);
            this.fileChannel = FileChannel.open(file, OPEN_OPTIONS);
            long written = 0;
            while (written < fileSize) {
                final int toWrite = Math.toIntExact(Math.min(fileSize - written, Channels.WRITE_CHUNK_SIZE));
                fillBytes.position(0).limit(toWrite);
                // write one byte at the end of the file to make sure all bytes are allocated
                Channels.writeToChannel(fillBytes, fileChannel);
                written += toWrite;
            }
        } else {
            this.fileChannel = null;
        }
    }

    public final class IO {

        private final long pageStart;

        private IO(final long sharedBytesPos) {
            pageStart = getPhysicalOffset(sharedBytesPos);
        }

        @SuppressForbidden(reason = "Use positional reads on purpose")
        public int read(ByteBuffer dst, long position) throws IOException {
            checkOffsets(position, dst.remaining());
            return fileChannel.read(dst, position);
        }

        @SuppressForbidden(reason = "Use positional writes on purpose")
        public int write(ByteBuffer src, long position) throws IOException {
            checkOffsets(position, src.remaining());
            return fileChannel.write(src, position);
        }

        private void checkOffsets(long position, long length) {
            long pageEnd = pageStart + regionSize;
            if (position < pageStart || position > pageEnd || position + length > pageEnd) {
                assert false;
                throw new IllegalArgumentException("bad access");
            }
        }
    }

    IO getFileChannel(int sharedBytesPos) {
        assert fileChannel != null;
        return new IO(sharedBytesPos);
    }

    long getPhysicalOffset(long chunkPosition) {
        long physicalOffset = chunkPosition * regionSize;
        assert physicalOffset <= numRegions * regionSize;
        return physicalOffset;
    }
}
