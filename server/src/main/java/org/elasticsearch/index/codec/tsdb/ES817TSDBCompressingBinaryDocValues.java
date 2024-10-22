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
    static final int MAX_CHUNK_SIZE = 256 * 1024;
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
            tmpBlockPointers.writeByte(numDocsInCurrentBlock);
            tmpBlockPointers.writeVLong(data.getFilePointer() - dataStartFilePointer);
            try (CompressingBuffer compressingBuffer = compress(block, numBytesInCurrentBlock)) {
                boolean allLengthsSame = true;
                for (int i = 1; i < docLengths.length; i++) {
                    if (docLengths[0] != docLengths[i]) {
                        allLengthsSame = false;
                        break;
                    }
                }
                byte header = compressingBuffer != null ? (byte) 1 : (byte) 0;
                if (allLengthsSame) {
                    header |= 2;
                }
                data.writeByte(header);
                final int docOffset = totalDocsWithValues - numDocsInCurrentBlock;
                assert docOffset >= 0 : docOffset;
                data.writeInt(docOffset);
                data.writeByte(numDocsInCurrentBlock);
                if (allLengthsSame) {
                    data.writeInt(docLengths[0]);
                } else {
                    int pos = 0;
                    for (int i = 0; i < numDocsInCurrentBlock; i++) {
                        pos += docLengths[i];
                        data.writeInt(pos);
                    }
                }
                if (compressingBuffer != null) {
                    final int compressedLen = compressingBuffer.compressedLen;
                    data.writeVInt(compressedLen);
                    compressingBuffer.buffer.buffer().get(block, 0, compressedLen);
                    data.writeBytes(block, 0, compressedLen);
                } else {
                    data.writeBytes(block, 0, numBytesInCurrentBlock);
                }
            }
            numDocsInCurrentBlock = 0;
            numBytesInCurrentBlock = 0;
            totalBlocks++;
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
                            byte numDocs = blockPointerIn.readByte();
                            final long blockOffset = blockPointerIn.readVLong();
                            for (int d = 0; d < numDocs; d++) {
                                blockPointers.add(blockOffset);
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
        long address = -1;
        byte numDocs;
        int docOffset = -1;
        boolean sameLength;
        boolean compressed;

        boolean contains(int docID) {
            return docOffset <= docID && docID < docOffset + numDocs;
        }
    }

    static final class Reader {
        static final int START_OFFSET_FP = 6;
        final LongValues blockAddresses;
        final IndexInput data;
        final BytesRef values = new BytesRef();
        private final Block block = new Block();
        private final boolean merging;
        private final int[] offsets;

        Reader(Entry entry, IndexInput data, boolean merging) throws IOException {
            final RandomAccessInput addressesData = data.randomAccessSlice(entry.blockAddressOffset, entry.blockAddressLength);
            this.blockAddresses = DirectMonotonicReader.getInstance(entry.blockAddressMeta, addressesData);
            this.data = data.slice("binary_values", entry.dataOffset, entry.dataLength);
            this.merging = merging;
            this.offsets = merging ? new int[MAX_DOC_PER_CHUNK + 1] : null;
        }

        BytesRef readValue(int docID) throws IOException {
            if (block.contains(docID) == false) {
                final long blockAddress = blockAddresses.get(docID);
                if (block.address != blockAddress) {
                    loadBlock(blockAddress);
                }
                assert block.contains(docID);
            }
            final int position = docID - block.docOffset;
            // load offset, length
            if (merging) {
                assert offsets != null;
                values.offset = offsets[position];
                final int length = offsets[position + 1] - values.offset;
                assert length >= 0 : length;
                values.length = length;
            } else {
                readOneOffsetAndLength(position);
            }
            if (block.compressed == false) {
                readOneValue();
            }
            return values;
        }

        void loadBlock(long startAddress) throws IOException {
            data.seek(startAddress);
            block.address = startAddress;
            final byte header = data.readByte();
            block.sameLength = (header & 2) != 0;
            block.compressed = (header & 1) != 0;
            block.docOffset = data.readInt();
            block.numDocs = data.readByte();
            // load all offsets of a merging instance
            if (merging) {
                assert offsets != null;
                if (block.sameLength) {
                    int docLength = data.readInt();
                    for (int i = 1; i <= block.numDocs; i++) {
                        offsets[i] = docLength * i;
                    }
                } else {
                    for (int i = 1; i <= block.numDocs; i++) {
                        offsets[i] = data.readInt();
                    }
                }
            }
            // decompress values
            if (block.compressed) {
                final int blockLength;
                if (merging) {
                    blockLength = offsets[block.numDocs];
                } else if (block.sameLength) {
                    blockLength = data.readInt() * block.numDocs;
                } else {
                    data.seek(startAddress + START_OFFSET_FP + (block.numDocs - 1L) * 4L);
                    blockLength = data.readInt();
                }
                values.bytes = ArrayUtil.growNoCopy(values.bytes, blockLength);
                decompress(data, values.bytes, blockLength);
            }
        }

        private void readOneOffsetAndLength(int position) throws IOException {
            if (block.sameLength) {
                data.seek(block.address + START_OFFSET_FP);
                values.length = data.readInt();
                values.offset = position * values.length;
            } else if (position == 0) {
                values.offset = 0;
                data.seek(block.address + START_OFFSET_FP);
                values.length = data.readInt();
            } else {
                data.seek(block.address + START_OFFSET_FP + (position - 1) * 4L);
                values.offset = data.readInt();
                values.length = data.readInt() - values.offset;
            }
        }

        private void readOneValue() throws IOException {
            final int numOffsets = block.sameLength ? 1 : block.numDocs;
            data.seek(block.address + START_OFFSET_FP + numOffsets * 4L + values.offset);
            values.bytes = ArrayUtil.growNoCopy(values.bytes, values.length);
            values.offset = 0;
            data.readBytes(values.bytes, 0, values.length);
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
                final int compressedLen = zstd.compress(dest, src, 3);
                if (compressedLen * 10 > inputLen * 9) {
                    // ignore compression if savings are less than 10%
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

    private static void decompress(IndexInput dataIn, byte[] buffer, int originalLength) throws IOException {
        final int compressedLen = dataIn.readVInt();
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
                throw new CorruptIndexException("Expected " + originalLength + " decompressed bytes, got " + decompressedLen, dataIn);
            }
            dest.buffer().get(0, buffer, 0, originalLength);
        }
    }
}
