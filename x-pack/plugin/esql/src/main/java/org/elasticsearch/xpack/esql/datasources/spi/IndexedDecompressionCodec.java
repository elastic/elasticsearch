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
 * Extension of {@link DecompressionCodec} for codecs that support index-based
 * random access to independently compressed frames. The canonical example is
 * the Zstandard seekable format, which appends a seek table as a skippable
 * frame at the end of the file.
 *
 * <p>When a codec implements this interface and an index is available for a
 * given file, the split provider can create splits aligned to frame boundaries
 * without scanning the compressed data for block markers. This is more efficient
 * than {@link SplittableDecompressionCodec} for formats that embed their own index.
 *
 * <p>If no index is available (e.g. the file was compressed without seekable
 * support), the codec falls back to stream-only decompression via
 * {@link DecompressionCodec#decompress(InputStream)}.
 */
public interface IndexedDecompressionCodec extends DecompressionCodec {

    /**
     * Checks whether the given storage object contains a frame index
     * (e.g. a seek table appended as a skippable frame).
     */
    boolean hasIndex(StorageObject object) throws IOException;

    /**
     * Reads the frame index from the storage object. The index describes
     * the compressed offset, compressed size, and decompressed size of
     * each independently decompressible frame.
     *
     * @throws IOException if the index cannot be read or is malformed
     * @throws UnsupportedOperationException if {@link #hasIndex} returns false
     */
    FrameIndex readIndex(StorageObject object) throws IOException;

    /**
     * Decompresses a single frame at the given compressed offset and length.
     * The returned stream yields the decompressed bytes for that frame only.
     *
     * @param object           the storage object containing the compressed data
     * @param compressedOffset byte offset of the frame in the compressed file
     * @param compressedLength byte length of the compressed frame
     * @return an input stream yielding decompressed bytes for the frame
     */
    InputStream decompressFrame(StorageObject object, long compressedOffset, long compressedLength) throws IOException;
}
