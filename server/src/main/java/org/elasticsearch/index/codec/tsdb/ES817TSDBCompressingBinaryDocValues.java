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
import org.elasticsearch.nativeaccess.CloseableByteBuffer;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.nativeaccess.Zstd;

import java.io.Closeable;
import java.io.IOException;

class ES817TSDBCompressingBinaryDocValues {
    static final int MAX_DOC_PER_CHUNK = 32;
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
        entry.blockAddressOffset = meta.readVLong();
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
        int numDocsInCurrentBlock = 0;
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
                flush(true);
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
                flush(false);
            }
        }

        void flush(boolean forced) throws IOException {
            boolean allLengthsSame = true;
            for (int i = 1; i < docLengths.length; i++) {
                if (docLengths[0] != docLengths[i]) {
                    allLengthsSame = false;
                    break;
                }
            }
            tmpBlockPointers.writeVInt(numDocsInCurrentBlock);
            tmpBlockPointers.writeVLong(data.getFilePointer() - dataStartFilePointer);
            if (allLengthsSame) {
                data.writeVInt((numDocsInCurrentBlock << 1) | 1);
                data.writeVInt(docLengths[0]);
            } else {
                data.writeVInt((numDocsInCurrentBlock << 1));
                for (int i = 0; i < numDocsInCurrentBlock; i++) {
                    data.writeVInt(docLengths[i]);
                }
            }
            final int docOffset = totalDocsWithValues - numDocsInCurrentBlock;
            assert docOffset >= 0 : docOffset;
            data.writeVInt(docOffset);
            if (numBytesInCurrentBlock > 0) {
                compress(block, numBytesInCurrentBlock, data);
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
                        meta.writeVLong(blockAddressesStart);
                        meta.writeVInt(ES817TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT);
                        final DirectMonotonicWriter blockPointers = DirectMonotonicWriter.getInstance(
                            meta,
                            data,
                            totalDocsWithValues,
                            ES817TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT
                        );
                        for (int b = 0; b < totalBlocks; ++b) {
                            int numDocs = blockPointerIn.readVInt();
                            final long blockOffset = blockPointerIn.readVLong();
                            for (int d = 0; d < numDocs; d++) {
                                blockPointers.add(blockOffset);
                            }
                        }
                        blockPointers.finish();
                        long blockAddressesLength = data.getFilePointer() - blockAddressesStart;
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
        int numDocs;
        int docOffset;
        int[] offsets = new int[MAX_DOC_PER_CHUNK + 1];
    }

    static final class Reader {
        final LongValues blockAddresses;
        final IndexInput data;
        final BytesRef values = new BytesRef();
        private final Block block = new Block();

        Reader(Entry entry, IndexInput data) throws IOException {
            final RandomAccessInput addressesData = data.randomAccessSlice(entry.blockAddressOffset, entry.blockAddressLength);
            this.blockAddresses = DirectMonotonicReader.getInstance(entry.blockAddressMeta, addressesData);
            this.data = data.slice("binary_values", entry.dataOffset, entry.dataLength);
        }

        BytesRef readValue(int docID) throws IOException {
            final long address = blockAddresses.get(docID);
            if (block.address != address) {
                loadBlock(address);
                block.address = address;
            }
            assert block.docOffset <= docID && docID < block.docOffset + block.numDocs;
            final int position = docID - block.docOffset;
            values.offset = block.offsets[position];
            final int length = block.offsets[position + 1] - values.offset;
            assert length >= 0 : length;
            values.length = length;
            return values;
        }

        void loadBlock(long startAddress) throws IOException {
            data.seek(startAddress);
            final int header = data.readVInt();
            block.numDocs = header >>> 1;
            if ((header & 1) == 0) {
                int docLength = 0;
                for (int i = 1; i <= block.numDocs; i++) {
                    docLength += data.readVInt();
                    block.offsets[i] = docLength;
                }
            } else {
                int docLength = data.readVInt();
                for (int i = 1; i <= block.numDocs; i++) {
                    block.offsets[i] = i * docLength;
                }
            }
            block.docOffset = data.readVInt();
            int totalDocLength = block.offsets[block.numDocs];
            values.bytes = ArrayUtil.growNoCopy(values.bytes, totalDocLength);
            if (totalDocLength > 0) {
                decompress(data, values.bytes, totalDocLength);
            }
        }
    }

    static void compress(byte[] input, int inputLen, IndexOutput dataOut) throws IOException {
        NativeAccess nativeAccess = NativeAccess.instance();
        Zstd zstd = nativeAccess.getZstd();
        int compressedBound = zstd.compressBound(inputLen);
        try (
            CloseableByteBuffer src = nativeAccess.newBuffer(inputLen);
            CloseableByteBuffer dest = nativeAccess.newBuffer(compressedBound)
        ) {
            src.buffer().put(input, 0, inputLen);
            src.buffer().flip();
            final int compressedLen = zstd.compress(dest, src, 3);
            dataOut.writeVInt(compressedLen);
            for (int written = 0; written < compressedLen;) {
                final int numBytes = Math.min(input.length, compressedLen - written);
                dest.buffer().get(input, 0, numBytes);
                dataOut.writeBytes(input, 0, numBytes);
                written += numBytes;
                assert written == dest.buffer().position();
            }
        }
    }

    static void decompress(IndexInput dataIn, byte[] buffer, int originalLength) throws IOException {
        final int compressedLen = dataIn.readVInt();
        final NativeAccess nativeAccess = NativeAccess.instance();
        final Zstd zstd = nativeAccess.getZstd();
        try (
            CloseableByteBuffer src = nativeAccess.newBuffer(compressedLen);
            CloseableByteBuffer dest = nativeAccess.newBuffer(originalLength)
        ) {
            while (src.buffer().position() < compressedLen) {
                final int numBytes = Math.min(buffer.length, compressedLen - src.buffer().position());
                dataIn.readBytes(buffer, 0, numBytes);
                src.buffer().put(buffer, 0, numBytes);
            }
            src.buffer().flip();
            final int decompressedLen = zstd.decompress(dest, src);
            if (decompressedLen != originalLength) {
                throw new CorruptIndexException("Expected " + originalLength + " decompressed bytes, got " + decompressedLen, dataIn);
            }
            dest.buffer().get(0, buffer, 0, originalLength);
        }
    }
}
