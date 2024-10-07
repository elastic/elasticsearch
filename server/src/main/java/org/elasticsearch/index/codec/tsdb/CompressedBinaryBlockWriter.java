/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.compress.LZ4;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.elasticsearch.core.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;

/**
 * Compresses binary doc values for {@link ES87TSDBDocValuesConsumer}
 */
final class CompressedBinaryBlockWriter implements Closeable {
    private final SegmentWriteState state;
    private final IndexOutput meta, data;
    private final long blockAddressesStart;
    private final IndexOutput tempBinaryOffsets;
    private final LZ4.FastCompressionHashTable ht = new LZ4.FastCompressionHashTable();

    int uncompressedBlockLength = 0;
    int maxUncompressedBlockLength = 0;
    int numDocsInCurrentBlock = 0;
    final int[] docLengths = new int[ES87TSDBDocValuesFormat.BINARY_DOCS_PER_COMPRESSED_BLOCK];
    byte[] block = BytesRef.EMPTY_BYTES;
    int totalChunks = 0;
    long maxPointer = 0;

    CompressedBinaryBlockWriter(SegmentWriteState state, IndexOutput meta, IndexOutput data) throws IOException {
        this.state = state;
        this.meta = meta;
        this.data = data;

        tempBinaryOffsets = state.directory.createTempOutput(state.segmentInfo.name, "binary_pointers", state.context);
        boolean success = false;
        try {
            CodecUtil.writeHeader(
                tempBinaryOffsets,
                ES87TSDBDocValuesFormat.META_CODEC + "FilePointers",
                ES87TSDBDocValuesFormat.VERSION_CURRENT
            );
            blockAddressesStart = data.getFilePointer();
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this); // self-close because constructor caller can't
            }
        }
    }

    void addDoc(BytesRef v) throws IOException {
        docLengths[numDocsInCurrentBlock] = v.length;
        block = ArrayUtil.grow(block, uncompressedBlockLength + v.length);
        System.arraycopy(v.bytes, v.offset, block, uncompressedBlockLength, v.length);
        uncompressedBlockLength += v.length;
        numDocsInCurrentBlock++;
        long blockSize = RamUsageEstimator.sizeOf(block);
        if (blockSize >= ES87TSDBDocValuesFormat.MAX_COMPRESSED_BLOCK_SIZE
            || numDocsInCurrentBlock == ES87TSDBDocValuesFormat.BINARY_DOCS_PER_COMPRESSED_BLOCK) {
            flushData();
        }
    }

    void flushData() throws IOException {
        if (numDocsInCurrentBlock > 0) {
            // Write offset to this block to temporary offsets file
            totalChunks++;
            long thisBlockStartPointer = data.getFilePointer();
            data.writeVInt(numDocsInCurrentBlock);
            // Optimisation - check if all lengths are same
            boolean allLengthsSame = true;
            for (int i = 1; i < numDocsInCurrentBlock; i++) {
                if (docLengths[i] != docLengths[i - 1]) {
                    allLengthsSame = false;
                    break;
                }
            }
            if (allLengthsSame) {
                // Only write one value shifted. Steal a bit to indicate all other lengths are the same
                int onlyOneLength = (docLengths[0] << 1) | 1;
                data.writeVInt(onlyOneLength);
            } else {
                for (int i = 0; i < numDocsInCurrentBlock; i++) {
                    if (i == 0) {
                        // Write first value shifted and steal a bit to indicate other lengths are to follow
                        int multipleLengths = (docLengths[0] << 1);
                        data.writeVInt(multipleLengths);
                    } else {
                        data.writeVInt(docLengths[i]);
                    }
                }
            }
            maxUncompressedBlockLength = Math.max(maxUncompressedBlockLength, uncompressedBlockLength);
            LZ4.compress(block, 0, uncompressedBlockLength, data, ht);
            numDocsInCurrentBlock = 0;
            // Ensure initialized with zeroes because full array is always written
            Arrays.fill(docLengths, 0);
            uncompressedBlockLength = 0;
            maxPointer = data.getFilePointer();
            tempBinaryOffsets.writeVLong(maxPointer - thisBlockStartPointer);
        }
    }

    void writeMetaData() throws IOException {
        if (totalChunks == 0) {
            return;
        }

        long startDMW = data.getFilePointer();
        meta.writeLong(startDMW);

        meta.writeVInt(totalChunks);
        meta.writeVInt(ES87TSDBDocValuesFormat.BINARY_BLOCK_SHIFT);
        meta.writeVInt(maxUncompressedBlockLength);
        meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);

        CodecUtil.writeFooter(tempBinaryOffsets);
        IOUtils.close(tempBinaryOffsets);
        // write the compressed block offsets info to the meta file by reading from temp file
        try (ChecksumIndexInput filePointersIn = state.directory.openChecksumInput(tempBinaryOffsets.getName(), IOContext.READONCE)) {
            CodecUtil.checkHeader(
                filePointersIn,
                ES87TSDBDocValuesFormat.META_CODEC + "FilePointers",
                ES87TSDBDocValuesFormat.VERSION_CURRENT,
                ES87TSDBDocValuesFormat.VERSION_CURRENT
            );
            Throwable priorE = null;
            try {
                final DirectMonotonicWriter filePointers = DirectMonotonicWriter.getInstance(
                    meta,
                    data,
                    totalChunks,
                    DIRECT_MONOTONIC_BLOCK_SHIFT
                );
                long fp = blockAddressesStart;
                for (int i = 0; i < totalChunks; ++i) {
                    filePointers.add(fp);
                    fp += filePointersIn.readVLong();
                }
                if (maxPointer < fp) {
                    throw new CorruptIndexException(
                        "File pointers don't add up (" + fp + " vs expected " + maxPointer + ")",
                        filePointersIn
                    );
                }
                filePointers.finish();
            } catch (Throwable e) {
                priorE = e;
            } finally {
                CodecUtil.checkFooter(filePointersIn, priorE);
            }
        }
        // Write the length of the DMW block in the data
        meta.writeLong(data.getFilePointer() - startDMW);
    }

    @Override
    public void close() throws IOException {
        if (tempBinaryOffsets != null) {
            IOUtils.close(tempBinaryOffsets);
            state.directory.deleteFile(tempBinaryOffsets.getName());
        }
    }

}
