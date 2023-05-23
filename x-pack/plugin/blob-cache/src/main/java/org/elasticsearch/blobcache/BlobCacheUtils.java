/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache;

import org.apache.lucene.store.IndexInput;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.core.Streams;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import static org.elasticsearch.core.Strings.format;

public class BlobCacheUtils {

    /**
     * We use {@code long} to represent offsets and lengths of files since they may be larger than 2GB, but {@code int} to represent
     * offsets and lengths of arrays in memory which are limited to 2GB in size. We quite often need to convert from the file-based world
     * of {@code long}s into the memory-based world of {@code int}s, knowing for certain that the result will not overflow. This method
     * should be used to clarify that we're doing this.
     */
    public static int toIntBytes(long l) {
        return ByteSizeUnit.BYTES.toIntBytes(l);
    }

    public static void throwEOF(long channelPos, long len, Object file) throws EOFException {
        throw new EOFException(format("unexpected EOF reading [%d-%d] from %s", channelPos, channelPos + len, file));
    }

    public static void ensureSeek(long pos, IndexInput input) throws IOException {
        final long length = input.length();
        if (pos > length) {
            throw new EOFException("Reading past end of file [position=" + pos + ", length=" + input.length() + "] for " + input);
        } else if (pos < 0L) {
            throw new IOException("Seeking to negative position [" + pos + "] for " + input);
        }
    }

    public static ByteRange computeRange(long rangeSize, long position, long size, long blobLength) {
        return ByteRange.of(
            (position / rangeSize) * rangeSize,
            Math.min((((position + size - 1) / rangeSize) + 1) * rangeSize, blobLength)
        );
    }

    public static void ensureSlice(String sliceName, long sliceOffset, long sliceLength, IndexInput input) {
        if (sliceOffset < 0 || sliceLength < 0 || sliceOffset + sliceLength > input.length()) {
            throw new IllegalArgumentException(
                "slice() "
                    + sliceName
                    + " out of bounds: offset="
                    + sliceOffset
                    + ",length="
                    + sliceLength
                    + ",fileLength="
                    + input.length()
                    + ": "
                    + input
            );
        }
    }

    /**
     * Perform a single {@code read()} from {@code inputStream} into {@code copyBuffer}, handling an EOF by throwing an {@link EOFException}
     * rather than returning {@code -1}. Returns the number of bytes read, which is always positive.
     *
     * Most of its arguments are there simply to make the message of the {@link EOFException} more informative.
     */
    public static int readSafe(InputStream inputStream, ByteBuffer copyBuffer, long rangeStart, long remaining, Object cacheFileReference)
        throws IOException {
        final int len = (remaining < copyBuffer.remaining()) ? toIntBytes(remaining) : copyBuffer.remaining();
        final int bytesRead = Streams.read(inputStream, copyBuffer, len);
        if (bytesRead <= 0) {
            throwEOF(rangeStart, remaining, cacheFileReference);
        }
        return bytesRead;
    }
}
