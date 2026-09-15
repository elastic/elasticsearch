/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LongValues;

import java.io.IOException;

/**
 * Raw, block-level view of one segment's compressed binary doc values field, used by merges that
 * splice whole blocks into the target segment rather than decompressing every value and compressing
 * it again.
 *
 * <p>Splicing is possible because a block's on-disk bytes are position independent: the header byte,
 * the uncompressed length and the doc offsets are all block relative, and the compressed payload is a
 * standalone frame. Everything segment-absolute lives in the block metadata, which the target rebuilds
 * from its own file pointers. A block is therefore byte-compatible with a target that agrees on the
 * {@link DocOffsetsCodec} used to encode its doc offsets and on the field-level
 * {@link BinaryDVCompressionMode}; neither is recorded inside the block itself, so the caller has to
 * compare them. Per-block compression need not agree, since that flag does live in the block header.
 *
 * <p>Instances are only produced for fields that are dense in their segment, which lets the merge treat
 * a source doc id as its index into the value stream.
 */
final class BinaryBlockSource {

    /** Field-level compression of the source, which must match the target's for a splice to decode. */
    final BinaryDVCompressionMode compression;
    /** Identity of the doc offsets encoding, which is not recorded on disk and so must match the target's. */
    final DocOffsetsCodec docOffsetsCodec;
    /** Number of compressed blocks this field occupies in the source segment. */
    final int numBlocks;

    /** Cumulative byte offsets, {@code numBlocks + 1} entries; entry {@code b} is where block {@code b} starts. */
    private final LongValues blockAddresses;
    /** Cumulative doc counts, {@code numBlocks + 1} entries; entry {@code b} is block {@code b}'s first value index. */
    private final LongValues blockDocRanges;
    /** Cursor used for bulk copying. */
    private final IndexInput data;
    /** Independent cursor for peeking block headers, so header reads never disturb an in-flight copy. */
    private final IndexInput headers;

    BinaryBlockSource(
        BinaryDVCompressionMode compression,
        DocOffsetsCodec docOffsetsCodec,
        int numBlocks,
        LongValues blockAddresses,
        LongValues blockDocRanges,
        IndexInput data
    ) {
        this.compression = compression;
        this.docOffsetsCodec = docOffsetsCodec;
        this.numBlocks = numBlocks;
        this.blockAddresses = blockAddresses;
        this.blockDocRanges = blockDocRanges;
        this.data = data;
        this.headers = data.clone();
    }

    /** Index into the value stream of the first document of {@code block}, which for a dense field is its doc id. */
    int firstValueIndex(int block) {
        return (int) blockDocRanges.get(block);
    }

    /** Number of documents stored in {@code block}. */
    int numDocs(int block) {
        return (int) (blockDocRanges.get(block + 1) - blockDocRanges.get(block));
    }

    /**
     * Uncompressed payload size of {@code block}, read from the block's own header rather than from the
     * field-level maximum, so the target can fold the exact value into its own {@code maxUncompressedBlockLength}.
     */
    int uncompressedLength(int block) throws IOException {
        headers.seek(blockAddresses.get(block));
        headers.readByte(); // BlockHeader, whose isCompressed flag travels with the copied bytes
        return headers.readVInt();
    }

    /** Appends {@code block} verbatim to {@code out}, including its header and encoded doc offsets. */
    long copyTo(IndexOutput out, int block) throws IOException {
        final long start = blockAddresses.get(block);
        final long length = blockAddresses.get(block + 1) - start;
        data.seek(start);
        out.copyBytes(data, length);
        return length;
    }
}
