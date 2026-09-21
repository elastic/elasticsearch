/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2026 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.lucene50.compressing;

import org.apache.lucene.backward_codecs.compressing.CompressionMode;
import org.apache.lucene.backward_codecs.packed.LegacyDirectMonotonicWriter;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;

/**
 * A {@link StoredFieldsFormat} that compresses documents in chunks in order to improve the
 * compression ratio.
 *
 * <p>For a chunk size of <var>chunkSize</var> bytes, this {@link StoredFieldsFormat} does not
 * support documents larger than (<code>2<sup>31</sup> - chunkSize</code>) bytes.
 *
 * <p>For optimal performance, you should use a {@link MergePolicy} that returns segments that have
 * the biggest byte size first.
 *
 */
public class Lucene50CompressingStoredFieldsFormat extends StoredFieldsFormat {

    /** format name */
    protected final String formatName;

    /** segment suffix */
    protected final String segmentSuffix;

    /** compression mode */
    protected final CompressionMode compressionMode;

    /** chunk size */
    protected final int chunkSize;

    /** max docs per chunk */
    protected final int maxDocsPerChunk;

    /** block shift */
    protected final int blockShift;

    /**
     * Create a new {@link Lucene50CompressingStoredFieldsFormat} with an empty segment suffix.
     *
     * @see Lucene50CompressingStoredFieldsFormat#Lucene50CompressingStoredFieldsFormat(String,
     *     String, CompressionMode, int, int, int)
     */
    public Lucene50CompressingStoredFieldsFormat(
        String formatName,
        CompressionMode compressionMode,
        int chunkSize,
        int maxDocsPerChunk,
        int blockShift
    ) {
        this(formatName, "", compressionMode, chunkSize, maxDocsPerChunk, blockShift);
    }

    /**
     * Create a new {@link Lucene50CompressingStoredFieldsFormat}.
     *
     * <p><code>formatName</code> is the name of the format. This name will be used in the file
     * formats to perform {@link CodecUtil#checkIndexHeader codec header checks}.
     *
     * <p><code>segmentSuffix</code> is the segment suffix. This suffix is added to the result file
     * name only if it's not the empty string.
     *
     * <p>The <code>compressionMode</code> parameter allows you to choose between compression
     * algorithms that have various compression and decompression speeds so that you can pick the one
     * that best fits your indexing and searching throughput. You should never instantiate two {@link
     * Lucene50CompressingStoredFieldsFormat}s that have the same name but different {@link
     * CompressionMode}s.
     *
     * <p><code>chunkSize</code> is the minimum byte size of a chunk of documents. A value of <code>1
     * </code> can make sense if there is redundancy across fields. <code>maxDocsPerChunk</code> is an
     * upperbound on how many docs may be stored in a single chunk. This is to bound the cpu costs for
     * highly compressible data.
     *
     * <p>Higher values of <code>chunkSize</code> should improve the compression ratio but will
     * require more memory at indexing time and might make document loading a little slower (depending
     * on the size of your OS cache compared to the size of your index).
     *
     * @param formatName the name of the {@link StoredFieldsFormat}
     * @param compressionMode the {@link CompressionMode} to use
     * @param chunkSize the minimum number of bytes of a single chunk of stored documents
     * @param maxDocsPerChunk the maximum number of documents in a single chunk
     * @param blockShift the log in base 2 of number of chunks to store in an index block
     * @see CompressionMode
     */
    public Lucene50CompressingStoredFieldsFormat(
        String formatName,
        String segmentSuffix,
        CompressionMode compressionMode,
        int chunkSize,
        int maxDocsPerChunk,
        int blockShift
    ) {
        this.formatName = formatName;
        this.segmentSuffix = segmentSuffix;
        this.compressionMode = compressionMode;
        if (chunkSize < 1) {
            throw new IllegalArgumentException("chunkSize must be >= 1");
        }
        this.chunkSize = chunkSize;
        if (maxDocsPerChunk < 1) {
            throw new IllegalArgumentException("maxDocsPerChunk must be >= 1");
        }
        this.maxDocsPerChunk = maxDocsPerChunk;
        if (blockShift < LegacyDirectMonotonicWriter.MIN_BLOCK_SHIFT || blockShift > LegacyDirectMonotonicWriter.MAX_BLOCK_SHIFT) {
            throw new IllegalArgumentException(
                "blockSize must be in "
                    + LegacyDirectMonotonicWriter.MIN_BLOCK_SHIFT
                    + "-"
                    + LegacyDirectMonotonicWriter.MAX_BLOCK_SHIFT
                    + ", got "
                    + blockShift
            );
        }
        this.blockShift = blockShift;
    }

    @Override
    public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
        return new Lucene50CompressingStoredFieldsReader(directory, si, segmentSuffix, fn, context, formatName, compressionMode);
    }

    @Override
    public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
        throw new UnsupportedOperationException("Old formats can't be used for writing");
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "(compressionMode="
            + compressionMode
            + ", chunkSize="
            + chunkSize
            + ", maxDocsPerChunk="
            + maxDocsPerChunk
            + ", blockShift="
            + blockShift
            + ")";
    }
}
