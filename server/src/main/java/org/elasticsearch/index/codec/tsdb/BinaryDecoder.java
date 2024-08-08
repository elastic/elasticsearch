/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.compress.LZ4;

import java.io.IOException;

/**
 * Decompresses binary doc values for {@link org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesProducer}
 */
final class BinaryDecoder {

    private final LongValues addresses;
    private final IndexInput compressedData;
    // Cache of last uncompressed block
    private long lastBlockId = -1;
    private final int[] uncompressedDocStarts;
    private int uncompressedBlockLength = 0;
    private final byte[] uncompressedBlock;
    private final BytesRef uncompressedBytesRef;
    private final int docsPerChunk;
    private final int docsPerChunkShift;

    BinaryDecoder(LongValues addresses, IndexInput compressedData, int biggestUncompressedBlockSize, int docsPerChunkShift) {
        super();
        this.addresses = addresses;
        this.compressedData = compressedData;
        // pre-allocate a byte array large enough for the biggest uncompressed block needed.
        this.uncompressedBlock = new byte[biggestUncompressedBlockSize];
        uncompressedBytesRef = new BytesRef(uncompressedBlock);
        this.docsPerChunk = 1 << docsPerChunkShift;
        this.docsPerChunkShift = docsPerChunkShift;
        uncompressedDocStarts = new int[docsPerChunk + 1];

    }

    BytesRef decode(int docId) throws IOException {
        int blockId = docId >> docsPerChunkShift;
        int docInBlockId = docId % docsPerChunk;
        assert docInBlockId < docsPerChunk;

        // already read and uncompressed?
        if (blockId != lastBlockId) {
            lastBlockId = blockId;
            long blockStartOffset = addresses.get(blockId);
            compressedData.seek(blockStartOffset);

            uncompressedBlockLength = 0;

            int docsPerChunk = compressedData.readVInt();

            int onlyLength = -1;
            for (int i = 0; i < docsPerChunk; i++) {
                if (i == 0) {
                    // The first length value is special. It is shifted and has a bit to denote if
                    // all other values are the same length
                    int lengthPlusSameInd = compressedData.readVInt();
                    int sameIndicator = lengthPlusSameInd & 1;
                    int firstValLength = lengthPlusSameInd >>> 1;
                    if (sameIndicator == 1) {
                        onlyLength = firstValLength;
                    }
                    uncompressedBlockLength += firstValLength;
                } else {
                    if (onlyLength == -1) {
                        // Various lengths are stored - read each from disk
                        uncompressedBlockLength += compressedData.readVInt();
                    } else {
                        // Only one length
                        uncompressedBlockLength += onlyLength;
                    }
                }
                uncompressedDocStarts[i + 1] = uncompressedBlockLength;
            }

            if (uncompressedBlockLength == 0) {
                uncompressedBytesRef.offset = 0;
                uncompressedBytesRef.length = 0;
                return uncompressedBytesRef;
            }

            assert uncompressedBlockLength <= uncompressedBlock.length;
            LZ4.decompress(compressedData, uncompressedBlockLength, uncompressedBlock, 0);
        }

        uncompressedBytesRef.offset = uncompressedDocStarts[docInBlockId];
        uncompressedBytesRef.length = uncompressedDocStarts[docInBlockId + 1] - uncompressedBytesRef.offset;
        return uncompressedBytesRef;
    }

}
