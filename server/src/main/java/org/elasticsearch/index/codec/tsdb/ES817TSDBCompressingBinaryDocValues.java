/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.nativeaccess.CloseableByteBuffer;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.Zstd;

import java.io.Closeable;
import java.io.IOException;

class ES817TSDBCompressingBinaryDocValues {
    static final byte MAX_DOC_PER_CHUNK = 32;
    static final int MAX_CHUNK_SIZE = 30 * 1024;
    static final String TMP_BLOCK_POINTERS_CODEC = "TSDB_817_BlockPointers";

    static final class Entry {
        // DISI
        long indexedDISIOffset;
        long indexedDISILength;
        short jumpTableEntryCount;
        byte denseRankPower;

        // block offsets
        int numDocsWithValues;
        long blockAddressOffset;
        long blockAddressLength;
        DirectMonotonicReader.Meta blockAddressMeta;

        // values
        long dataOffset;
        long dataLength;
    }

    static Entry readEntry(IndexInput meta) throws IOException {
        Entry entry = new Entry();
        entry.dataOffset = meta.readVLong();
        entry.dataLength = meta.readVLong();
        entry.indexedDISIOffset = meta.readLong();
        if (entry.indexedDISIOffset >= 0) {
            entry.indexedDISILength = meta.readVLong();
            entry.jumpTableEntryCount = meta.readShort();
            entry.denseRankPower = meta.readByte();
        }
        // block addresses
        entry.numDocsWithValues = meta.readVInt();
        entry.blockAddressOffset = meta.readLong();
        final int blockShift = meta.readVInt();
        entry.blockAddressMeta = DirectMonotonicReader.loadMeta(meta, entry.numDocsWithValues, blockShift);
        entry.blockAddressLength = meta.readVLong();
        return entry;
    }

    static final class Writer implements Closeable {
        final SegmentWriteState state;
        final IndexOutput data;
        final IndexOutput meta;

        final int[] docLengths = new int[MAX_DOC_PER_CHUNK];
        byte[] block = BytesRef.EMPTY_BYTES;
        byte numDocsInCurrentBlock = 0;
        int numBytesInCurrentBlock = 0;
        int totalBlocks = 0;
        int totalDocsWithValues = 0;
        final long dataStartFilePointer;

        private IndexOutput tmpBlockPointers;

        Writer(SegmentWriteState state, IndexOutput data, IndexOutput meta) throws IOException {
            this.state = state;
            this.data = data;
            this.meta = meta;
            this.dataStartFilePointer = data.getFilePointer();
            tmpBlockPointers = state.directory.createTempOutput(state.segmentInfo.name, "tsdb_block_pointers", state.context);
            boolean success = false;
            try {
                CodecUtil.writeHeader(tmpBlockPointers, TMP_BLOCK_POINTERS_CODEC, ES817TSDBDocValuesFormat.VERSION_CURRENT);
                success = true;
            } finally {
                if (success == false) {
                    tmpBlockPointers.close();
                }
            }
        }

        void add(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            BinaryDocValues values = valuesProducer.getBinary(field);
            for (int docID = values.nextDoc(); docID != DocIdSetIterator.NO_MORE_DOCS; docID = values.nextDoc()) {
                totalDocsWithValues++;
                addDoc(values.binaryValue());
            }
            assert totalDocsWithValues <= state.segmentInfo.maxDoc();
            if (numDocsInCurrentBlock > 0) {
                flush();
            }
            meta.writeVLong(dataStartFilePointer);
            meta.writeVLong(data.getFilePointer() - dataStartFilePointer); // dataLength
            if (totalDocsWithValues == 0) {
                meta.writeLong(-2); // indexedDISIOffset
            } else if (totalDocsWithValues == state.segmentInfo.maxDoc()) {
                meta.writeLong(-1); // indexedDISIOffset
            } else {
                long offset = data.getFilePointer(); // We can store
                meta.writeLong(offset); // indexedDISIOffset
                values = valuesProducer.getBinary(field);
                final short jumpTableEntryCount = IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
                long indexedDISILength = data.getFilePointer() - offset;
                meta.writeVLong(indexedDISILength); // indexedDISILength
                meta.writeShort(jumpTableEntryCount);
                meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
            }
            finish();
        }

        void addDoc(BytesRef v) throws IOException {
            docLengths[numDocsInCurrentBlock] = v.length;
            block = ArrayUtil.grow(block, numBytesInCurrentBlock + v.length);
            System.arraycopy(v.bytes, v.offset, block, numBytesInCurrentBlock, v.length);
            numBytesInCurrentBlock += v.length;
            numDocsInCurrentBlock++;
            if (numDocsInCurrentBlock >= MAX_DOC_PER_CHUNK || numBytesInCurrentBlock >= MAX_CHUNK_SIZE) {
                flush();
            }
        }

        void flush() throws IOException {
            try (CompressingBuffer compressingBuffer = compress(block, numBytesInCurrentBlock)) {
                if (compressingBuffer != null) {
                    tmpBlockPointers.writeByte(numDocsInCurrentBlock);
                    tmpBlockPointers.writeVLong(data.getFilePointer() - dataStartFilePointer);
                    final int docOffset = totalDocsWithValues - numDocsInCurrentBlock;
                    assert docOffset >= 0 : docOffset;
                    data.writeByte((byte) (docOffset % MAX_DOC_PER_CHUNK));
                    data.writeByte(numDocsInCurrentBlock);
                    for (int i = 0; i < numDocsInCurrentBlock; i++) {
                        data.writeVInt(docLengths[i]);
                    }
                    final int compressedLen = compressingBuffer.compressedLen;
                    compressingBuffer.buffer.buffer().get(block, 0, compressedLen);
                    data.writeBytes(block, 0, compressedLen);
                    totalBlocks++;
                } else {
                    long startFP = data.getFilePointer() - dataStartFilePointer;
                    data.writeBytes(block, 0, numBytesInCurrentBlock);
                    int offset = 0;
                    for (byte i = 0; i < numDocsInCurrentBlock; i++) {
                        tmpBlockPointers.writeByte((byte) 0);
                        tmpBlockPointers.writeVLong(startFP + offset);
                        offset += docLengths[i];
                    }
                    totalBlocks += numDocsInCurrentBlock;
                }
            }
            numDocsInCurrentBlock = 0;
            numBytesInCurrentBlock = 0;
        }

        void finish() throws IOException {
            CodecUtil.writeFooter(tmpBlockPointers);
            String fileName = tmpBlockPointers.getName();
            try {
                tmpBlockPointers.close();
                try (var blockPointerIn = state.directory.openChecksumInput(fileName)) {
                    CodecUtil.checkHeader(
                        blockPointerIn,
                        TMP_BLOCK_POINTERS_CODEC,
                        ES817TSDBDocValuesFormat.VERSION_CURRENT,
                        ES817TSDBDocValuesFormat.VERSION_CURRENT
                    );
                    Throwable priorE = null;
                    try {
                        final long blockAddressesStart = data.getFilePointer();
                        meta.writeVInt(totalDocsWithValues);
                        meta.writeLong(blockAddressesStart);
                        meta.writeVInt(ES817TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT);
                        final DirectMonotonicWriter blockPointers = DirectMonotonicWriter.getInstance(
                            meta,
                            data,
                            totalDocsWithValues,
                            ES817TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT
                        );
                        for (int b = 0; b < totalBlocks; ++b) {
                            int numDocs = blockPointerIn.readByte();
                            long blockOffset = blockPointerIn.readVLong();
                            if (numDocs == 0) {
                                blockOffset = blockOffset << 1;
                                blockPointers.add(blockOffset);
                            } else {
                                blockOffset = (blockOffset << 1) | 1;
                                for (int d = 0; d < numDocs; d++) {
                                    blockPointers.add(blockOffset);
                                }
                            }
                        }
                        blockPointers.finish();
                        final long blockAddressesLength = data.getFilePointer() - blockAddressesStart;
                        meta.writeVLong(blockAddressesLength);
                    } catch (Throwable e) {
                        priorE = e;
                    } finally {
                        CodecUtil.checkFooter(blockPointerIn, priorE);
                    }
                }
            } finally {
                this.tmpBlockPointers = null;
                state.directory.deleteFile(fileName);
            }
        }

        @Override
        public void close() throws IOException {
            if (tmpBlockPointers != null) {
                IOUtils.close(tmpBlockPointers, () -> state.directory.deleteFile(tmpBlockPointers.getName()));
            }
        }
    }

    private static class Block {
        int numDocs;
        int firstDocId = Integer.MAX_VALUE;
        boolean compressed;

        boolean contains(int docID) {
            return firstDocId <= docID && docID < firstDocId + numDocs;
        }
    }

    static final class Reader {
        final Entry entry;
        final LongValues blockAddresses;
        final IndexInput data;
        final BytesRef values = new BytesRef();
        private final Block block = new Block();
        private final int[] offsets;

        Reader(Entry entry, IndexInput data) throws IOException {
            this.entry = entry;
            final RandomAccessInput addressesData = data.randomAccessSlice(entry.blockAddressOffset, entry.blockAddressLength);
            this.blockAddresses = DirectMonotonicReader.getInstance(entry.blockAddressMeta, addressesData);
            this.data = data.slice("binary_values", entry.dataOffset, entry.dataLength);
            this.offsets = new int[MAX_DOC_PER_CHUNK + 1];
        }

        BytesRef readValue(int docID) throws IOException {
            if (block.contains(docID) == false) {
                loadBlock(docID);
                assert block.contains(docID);
            }
            if (block.compressed) {
                final int docPosition = docID - block.firstDocId;
                values.offset = offsets[docPosition];
                final int length = offsets[docPosition + 1] - values.offset;
                assert length >= 0 : length;
                values.length = length;
            }
            return values;
        }

        private void loadBlock(int docID) throws IOException {
            final long startBlock = blockAddresses.get(docID);
            final long blockAddress = startBlock >>> 1;
            data.seek(blockAddress);
            block.compressed = (startBlock & 1) != 0;
            if (block.compressed) {
                final int offset = data.readByte();
                block.firstDocId = docID - ((docID - offset) % MAX_DOC_PER_CHUNK);
                block.numDocs = data.readByte();
                for (int i = 1; i <= block.numDocs; i++) {
                    offsets[i] = offsets[i - 1] + data.readVInt();
                }
                final int headerBytes = Math.toIntExact(data.getFilePointer() - blockAddress);
                final int decompressedLen = blockBytes(blockAddress, docID + block.numDocs) - headerBytes;
                values.length = offsets[block.numDocs];
                values.bytes = ArrayUtil.growNoCopy(values.bytes, values.length);
                decompress(data, values.bytes, decompressedLen, values.length);
            } else {
                // single doc block
                block.firstDocId = docID;
                block.numDocs = 1;
                values.offset = 0;
                values.length = blockBytes(blockAddress, docID + 1);
                values.bytes = ArrayUtil.growNoCopy(values.bytes, values.length);
                data.readBytes(values.bytes, 0, values.length);
            }
        }

        private int blockBytes(long currentBlockAddress, int firstDocIdNextBlock) {
            if (firstDocIdNextBlock >= entry.numDocsWithValues) {
                return Math.toIntExact(entry.dataLength - currentBlockAddress);
            } else {
                final long nextBlockAddress = blockAddresses.get(firstDocIdNextBlock) >>> 1;
                return Math.toIntExact(nextBlockAddress - currentBlockAddress);
            }
        }
    }

    private record CompressingBuffer(int compressedLen, CloseableByteBuffer buffer) implements Releasable {
        @Override
        public void close() {
            buffer.close();
        }
    }

    private static CompressingBuffer compress(byte[] input, int inputLen) {
        // don't compress small blocks
        if (inputLen < 1024) {
            return null;
        }
        NativeAccess nativeAccess = NativeAccess.instance();
        Zstd zstd = nativeAccess.getZstd();
        int compressedBound = zstd.compressBound(inputLen);
        try (CloseableByteBuffer src = nativeAccess.newBuffer(inputLen)) {
            src.buffer().put(input, 0, inputLen);
            src.buffer().flip();
            CloseableByteBuffer dest = nativeAccess.newBuffer(compressedBound);
            try {
                final int compressedLen = zstd.compress(dest, src, 1);
                if (compressedLen * 10 > inputLen * 8) {
                    // ignore compression if savings are less than 20%
                    return null;
                }
                var result = new CompressingBuffer(compressedLen, dest);
                dest = null;
                return result;
            } finally {
                if (dest != null) {
                    dest.close();
                }
            }
        }
    }

    private static void decompress(IndexInput dataIn, byte[] buffer, int compressedLen, int originalLength) throws IOException {
        final NativeAccess nativeAccess = NativeAccess.instance();
        final Zstd zstd = nativeAccess.getZstd();
        try (
            CloseableByteBuffer src = nativeAccess.newBuffer(compressedLen);
            CloseableByteBuffer dest = nativeAccess.newBuffer(originalLength)
        ) {
            dataIn.readBytes(buffer, 0, compressedLen);
            src.buffer().put(buffer, 0, compressedLen);
            src.buffer().flip();
            final int decompressedLen = zstd.decompress(dest, src);
            if (decompressedLen != originalLength) {
                throw new CorruptIndexException("expected " + originalLength + " bytes, got " + decompressedLen, dataIn);
            }
            dest.buffer().get(0, buffer, 0, originalLength);
        }
    }
}
