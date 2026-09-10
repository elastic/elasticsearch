/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.io.IOException;
import java.io.InputStream;

/**
 * Extension of {@link DecompressionCodec} for codecs that support splitting compressed
 * files into independently decompressible ranges aligned to compressed block boundaries.
 *
 * <p>Bzip2 is the canonical example: each bzip2 block starts with a 48-bit magic marker
 * ({@code 0x314159265359}) and can be decompressed independently when preceded by a
 * synthetic stream header. This enables parallel decompression of large compressed files.
 *
 * <p>Stream-only codecs (gzip, zstd) cannot implement this interface because their
 * compressed blocks depend on previous state.
 */
public interface SplittableDecompressionCodec extends DecompressionCodec {

    /**
     * Finds compressed block boundaries within the given byte range of a storage object.
     * Returns byte offsets (in the compressed stream) where blocks start.
     *
     * <p>Returns an empty array when {@code start >= end} or when the range contains
     * no block boundaries (e.g. header-only or empty files).
     *
     * @param object the storage object to scan
     * @param start  start byte offset in the compressed file (inclusive)
     * @param end    end byte offset in the compressed file (exclusive)
     * @return sorted array of byte offsets where compressed blocks begin
     */
    long[] findBlockBoundaries(StorageObject object, long start, long end) throws IOException;

    /**
     * Decompresses a range of compressed blocks. The returned stream yields decompressed
     * bytes for blocks starting at {@code blockStart} up to (but not including) the block
     * at {@code nextBlockStart}.
     *
     * <p>For bzip2, this creates a synthetic stream by prepending the file header
     * ({@code BZh} + block size digit) to the raw block data, then wrapping in a
     * standard decompressor.
     *
     * <p>The caller is responsible for closing the returned stream.
     *
     * @param object         the storage object containing the compressed data
     * @param blockStart     byte offset of the first block to decompress
     * @param nextBlockStart byte offset of the next block (or file length for the last block);
     *                       must be greater than {@code blockStart}
     * @return an input stream yielding decompressed bytes for the specified block range
     * @throws IllegalArgumentException if {@code blockStart >= nextBlockStart}
     */
    InputStream decompressRange(StorageObject object, long blockStart, long nextBlockStart) throws IOException;
}
