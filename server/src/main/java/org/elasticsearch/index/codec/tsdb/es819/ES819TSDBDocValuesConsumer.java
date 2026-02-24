/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.compress.fsst.FSST;
import org.elasticsearch.common.compress.fsst.ReservoirSampler;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.index.codec.tsdb.es819.DocValuesConsumerUtil.compatibleWithOptimizedMerge;
import static org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat.BLOCK_BYTES_THRESHOLD;
import static org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat.BLOCK_COUNT_THRESHOLD;
import static org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat.SKIP_INDEX_LEVEL_SHIFT;
import static org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat.SKIP_INDEX_MAX_LEVEL;
import static org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat.SORTED_SET;

final class ES819TSDBDocValuesConsumer extends XDocValuesConsumer {

    final Directory dir;
    final IOContext context;
    IndexOutput data, meta;
    final int maxDoc;
    private final int skipIndexIntervalSize;
    private final int minDocsPerOrdinalForOrdinalRangeEncoding;
    final boolean enableOptimizedMerge;
    private final int primarySortFieldNumber;
    private final int numericBlockShift;
    private final int numericBlockSize;
    final SegmentWriteState state;
    final BinaryDVCompressionMode binaryDVCompressionMode;
    private final boolean enablePerBlockCompression; // only false for testing

    ES819TSDBDocValuesConsumer(
        BinaryDVCompressionMode binaryDVCompressionMode,
        final boolean enablePerBlockCompression,
        SegmentWriteState state,
        int skipIndexIntervalSize,
        int minDocsPerOrdinalForOrdinalRangeEncoding,
        boolean enableOptimizedMerge,
        int numericBlockShift,
        String dataCodec,
        String dataExtension,
        String metaCodec,
        String metaExtension
    ) throws IOException {
        this.binaryDVCompressionMode = binaryDVCompressionMode;
        this.enablePerBlockCompression = enablePerBlockCompression;
        this.state = state;
        this.dir = state.directory;
        this.minDocsPerOrdinalForOrdinalRangeEncoding = minDocsPerOrdinalForOrdinalRangeEncoding;
        this.primarySortFieldNumber = ES819TSDBDocValuesProducer.primarySortFieldNumber(state.segmentInfo, state.fieldInfos);
        this.context = state.context;
        this.numericBlockShift = numericBlockShift;
        this.numericBlockSize = 1 << numericBlockShift;

        boolean success = false;
        try {
            final String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
            data = state.directory.createOutput(dataName, state.context);
            CodecUtil.writeIndexHeader(
                data,
                dataCodec,
                ES819TSDBDocValuesFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );

            String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
            meta = state.directory.createOutput(metaName, state.context);
            CodecUtil.writeIndexHeader(
                meta,
                metaCodec,
                ES819TSDBDocValuesFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            meta.writeByte((byte) numericBlockShift);

            maxDoc = state.segmentInfo.maxDoc();
            this.skipIndexIntervalSize = skipIndexIntervalSize;
            this.enableOptimizedMerge = enableOptimizedMerge;
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        meta.writeInt(field.number);
        meta.writeByte(ES819TSDBDocValuesFormat.NUMERIC);
        var producer = new TsdbDocValuesProducer(valuesProducer) {
            @Override
            public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
                return DocValues.singleton(valuesProducer.getNumeric(field));
            }
        };
        if (field.docValuesSkipIndexType() != DocValuesSkipIndexType.NONE) {
            writeSkipIndex(field, producer);
        }

        writeField(field, producer, -1, null);
    }

    private boolean shouldEncodeOrdinalRange(FieldInfo field, long maxOrd, int numDocsWithValue, long numValues) {
        return maxDoc > 1
            && field.number == primarySortFieldNumber
            && numDocsWithValue == numValues // Only single valued fields can be supported with range encoded ordinals format
            && (numDocsWithValue / maxOrd) >= minDocsPerOrdinalForOrdinalRangeEncoding;
    }

    private long[] writeField(FieldInfo field, TsdbDocValuesProducer valuesProducer, long maxOrd, OffsetsAccumulator offsetsAccumulator)
        throws IOException {
        int numDocsWithValue = 0;
        long numValues = 0;

        SortedNumericDocValues values;
        if (valuesProducer.mergeStats.supported()) {
            numDocsWithValue = valuesProducer.mergeStats.sumNumDocsWithField();
            numValues = valuesProducer.mergeStats.sumNumValues();
        } else {
            values = valuesProducer.getSortedNumeric(field);
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                numDocsWithValue++;
                final int count = values.docValueCount();
                numValues += count;
            }
        }

        meta.writeLong(numValues);
        meta.writeInt(numDocsWithValue);

        DISIAccumulator disiAccumulator = null;
        try {
            if (numValues > 0) {
                assert numDocsWithValue > 0;
                final ByteBuffersDataOutput indexOut = new ByteBuffersDataOutput();
                DirectMonotonicWriter indexWriter = null;

                final long valuesDataOffset = data.getFilePointer();
                if (maxOrd == 1) {
                    // Special case for maxOrd of 1, signal -1 that no blocks will be written
                    meta.writeInt(-1);
                } else if (shouldEncodeOrdinalRange(field, maxOrd, numDocsWithValue, numValues)) {
                    assert offsetsAccumulator == null;
                    // When a field is sorted, use ordinal range encode for long runs of the same ordinal.
                    meta.writeInt(-2);
                    meta.writeVInt(Math.toIntExact(maxOrd));
                    meta.writeByte((byte) ES819TSDBDocValuesFormat.ORDINAL_RANGE_ENCODING_BLOCK_SHIFT);
                    values = valuesProducer.getSortedNumeric(field);
                    if (valuesProducer.mergeStats.supported() && numDocsWithValue < maxDoc) {
                        disiAccumulator = new DISIAccumulator(dir, context, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
                    }
                    DirectMonotonicWriter startDocs = DirectMonotonicWriter.getInstance(
                        meta,
                        data,
                        maxOrd + 1,
                        ES819TSDBDocValuesFormat.ORDINAL_RANGE_ENCODING_BLOCK_SHIFT
                    );
                    long lastOrd = 0;
                    startDocs.add(0);
                    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                        if (disiAccumulator != null) {
                            disiAccumulator.addDocId(doc);
                        }
                        final long nextOrd = values.nextValue();
                        if (nextOrd != lastOrd) {
                            lastOrd = nextOrd;
                            startDocs.add(doc);
                        }
                    }
                    startDocs.add(maxDoc);
                    startDocs.finish();
                } else {
                    indexWriter = DirectMonotonicWriter.getInstance(
                        meta,
                        new ByteBuffersIndexOutput(indexOut, "temp-dv-index", "temp-dv-index"),
                        1L + ((numValues - 1) >>> numericBlockShift),
                        ES819TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT
                    );
                    meta.writeInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
                    final long[] buffer = new long[numericBlockSize];
                    int bufferSize = 0;
                    final TSDBDocValuesEncoder encoder = new TSDBDocValuesEncoder(numericBlockSize);
                    values = valuesProducer.getSortedNumeric(field);
                    final int bitsPerOrd = maxOrd >= 0 ? PackedInts.bitsRequired(maxOrd - 1) : -1;
                    if (valuesProducer.mergeStats.supported() && numDocsWithValue < maxDoc) {
                        disiAccumulator = new DISIAccumulator(dir, context, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
                    }
                    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                        if (disiAccumulator != null) {
                            disiAccumulator.addDocId(doc);
                        }
                        final int count = values.docValueCount();
                        if (offsetsAccumulator != null) {
                            offsetsAccumulator.addDoc(count);
                        }
                        for (int i = 0; i < count; ++i) {
                            buffer[bufferSize++] = values.nextValue();
                            if (bufferSize == numericBlockSize) {
                                indexWriter.add(data.getFilePointer() - valuesDataOffset);
                                if (maxOrd >= 0) {
                                    encoder.encodeOrdinals(buffer, data, bitsPerOrd);
                                } else {
                                    encoder.encode(buffer, data);
                                }
                                bufferSize = 0;
                            }
                        }
                    }
                    if (bufferSize > 0) {
                        indexWriter.add(data.getFilePointer() - valuesDataOffset);
                        // Fill unused slots in the block with zeroes rather than junk
                        Arrays.fill(buffer, bufferSize, numericBlockSize, 0L);
                        if (maxOrd >= 0) {
                            encoder.encodeOrdinals(buffer, data, bitsPerOrd);
                        } else {
                            encoder.encode(buffer, data);
                        }
                    }
                }

                final long valuesDataLength = data.getFilePointer() - valuesDataOffset;
                if (indexWriter != null) {
                    indexWriter.finish();
                }
                final long indexDataOffset = data.getFilePointer();
                data.copyBytes(indexOut.toDataInput(), indexOut.size());
                meta.writeLong(indexDataOffset);
                meta.writeLong(data.getFilePointer() - indexDataOffset);

                meta.writeLong(valuesDataOffset);
                meta.writeLong(valuesDataLength);
            }

            if (numDocsWithValue == 0) { // meta[-2, 0]: No documents with values
                meta.writeLong(-2); // docsWithFieldOffset
                meta.writeLong(0L); // docsWithFieldLength
                meta.writeShort((short) -1); // jumpTableEntryCount
                meta.writeByte((byte) -1); // denseRankPower
            } else if (numDocsWithValue == maxDoc) { // meta[-1, 0]: All documents have values
                meta.writeLong(-1); // docsWithFieldOffset
                meta.writeLong(0L); // docsWithFieldLength
                meta.writeShort((short) -1); // jumpTableEntryCount
                meta.writeByte((byte) -1); // denseRankPower
            } else { // meta[data.offset, data.length]: IndexedDISI structure for documents with values
                long offset = data.getFilePointer();
                meta.writeLong(offset); // docsWithFieldOffset
                final short jumpTableEntryCount;
                if (maxOrd != 1 && disiAccumulator != null) {
                    jumpTableEntryCount = disiAccumulator.build(data);
                } else {
                    values = valuesProducer.getSortedNumeric(field);
                    jumpTableEntryCount = IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
                }
                meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
                meta.writeShort(jumpTableEntryCount);
                meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
            }
        } finally {
            IOUtils.close(disiAccumulator);
        }

        return new long[] { numDocsWithValue, numValues };
    }

    @Override
    public void mergeNumericField(FieldInfo mergeFieldInfo, MergeState mergeState) throws IOException {
        var result = compatibleWithOptimizedMerge(enableOptimizedMerge, mergeState, mergeFieldInfo);
        if (result.supported()) {
            mergeNumericField(result, mergeFieldInfo, mergeState);
        } else {
            super.mergeNumericField(mergeFieldInfo, mergeState);
        }
    }

    @Override
    public void mergeBinaryField(FieldInfo mergeFieldInfo, MergeState mergeState) throws IOException {
        var result = compatibleWithOptimizedMerge(enableOptimizedMerge, mergeState, mergeFieldInfo);
        if (result.supported()) {
            mergeBinaryField(result, mergeFieldInfo, mergeState);
        } else {
            super.mergeBinaryField(mergeFieldInfo, mergeState);
        }
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        meta.writeInt(field.number);
        meta.writeByte(ES819TSDBDocValuesFormat.BINARY);
        meta.writeByte(binaryDVCompressionMode.code);

        if (valuesProducer instanceof TsdbDocValuesProducer tsdbValuesProducer && tsdbValuesProducer.mergeStats.supported()) {
            final int numDocsWithField = tsdbValuesProducer.mergeStats.sumNumDocsWithField();
            final int minLength = tsdbValuesProducer.mergeStats.minLength();
            final int maxLength = tsdbValuesProducer.mergeStats.maxLength();

            assert numDocsWithField <= maxDoc;

            BinaryDocValues values = valuesProducer.getBinary(field);
            long start = data.getFilePointer();
            meta.writeLong(start); // dataOffset

            DISIAccumulator disiAccumulator = null;
            BinaryWriter binaryWriter = null;
            try {
                if (numDocsWithField > 0 && numDocsWithField < maxDoc) {
                    disiAccumulator = new DISIAccumulator(dir, context, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
                }

                assert maxLength >= minLength;
                if (binaryDVCompressionMode == BinaryDVCompressionMode.NO_COMPRESS) {
                    var offsetsAccumulator = maxLength > minLength ? new OffsetsAccumulator(dir, context, data, numDocsWithField) : null;
                    binaryWriter = new DirectBinaryWriter(offsetsAccumulator, null);
                } else {
                    binaryWriter = new CompressedBinaryBlockWriter(binaryDVCompressionMode);
                }

                for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                    BytesRef v = values.binaryValue();
                    binaryWriter.addDoc(v);
                    if (disiAccumulator != null) {
                        disiAccumulator.addDocId(doc);
                    }
                }
                binaryWriter.flushData();
                meta.writeLong(data.getFilePointer() - start); // dataLength

                if (numDocsWithField == 0) {
                    meta.writeLong(-2); // docsWithFieldOffset
                    meta.writeLong(0L); // docsWithFieldLength
                    meta.writeShort((short) -1); // jumpTableEntryCount
                    meta.writeByte((byte) -1); // denseRankPower
                } else if (numDocsWithField == maxDoc) {
                    meta.writeLong(-1); // docsWithFieldOffset
                    meta.writeLong(0L); // docsWithFieldLength
                    meta.writeShort((short) -1); // jumpTableEntryCount
                    meta.writeByte((byte) -1); // denseRankPower
                } else {
                    long offset = data.getFilePointer();
                    meta.writeLong(offset); // docsWithFieldOffset
                    final short jumpTableEntryCount = disiAccumulator.build(data);
                    meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
                    meta.writeShort(jumpTableEntryCount);
                    meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
                }

                meta.writeInt(numDocsWithField);
                meta.writeInt(minLength);
                meta.writeInt(maxLength);

                binaryWriter.writeAddressMetadata(minLength, maxLength, numDocsWithField);
            } finally {
                IOUtils.close(disiAccumulator, binaryWriter);
            }
        } else {
            BinaryWriter binaryWriter = null;
            try {
                if (binaryDVCompressionMode == BinaryDVCompressionMode.NO_COMPRESS) {
                    binaryWriter = new DirectBinaryWriter(null, valuesProducer.getBinary(field));
                } else {
                    binaryWriter = new CompressedBinaryBlockWriter(binaryDVCompressionMode);
                }

                BinaryDocValues values = valuesProducer.getBinary(field);
                long start = data.getFilePointer();
                meta.writeLong(start); // dataOffset
                int numDocsWithField = 0;
                int minLength = Integer.MAX_VALUE;
                int maxLength = 0;
                for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                    numDocsWithField++;
                    BytesRef v = values.binaryValue();
                    int length = v.length;
                    binaryWriter.addDoc(v);
                    minLength = Math.min(length, minLength);
                    maxLength = Math.max(length, maxLength);
                }
                binaryWriter.flushData();

                assert numDocsWithField <= maxDoc;
                meta.writeLong(data.getFilePointer() - start); // dataLength

                if (numDocsWithField == 0) {
                    meta.writeLong(-2); // docsWithFieldOffset
                    meta.writeLong(0L); // docsWithFieldLength
                    meta.writeShort((short) -1); // jumpTableEntryCount
                    meta.writeByte((byte) -1); // denseRankPower
                } else if (numDocsWithField == maxDoc) {
                    meta.writeLong(-1); // docsWithFieldOffset
                    meta.writeLong(0L); // docsWithFieldLength
                    meta.writeShort((short) -1); // jumpTableEntryCount
                    meta.writeByte((byte) -1); // denseRankPower
                } else {
                    long offset = data.getFilePointer();
                    meta.writeLong(offset); // docsWithFieldOffset
                    values = valuesProducer.getBinary(field);
                    final short jumpTableEntryCount = IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
                    meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
                    meta.writeShort(jumpTableEntryCount);
                    meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
                }

                meta.writeInt(numDocsWithField);
                meta.writeInt(minLength);
                meta.writeInt(maxLength);

                binaryWriter.writeAddressMetadata(minLength, maxLength, numDocsWithField);
            } finally {
                IOUtils.close(binaryWriter);
            }
        }
    }

    private sealed interface BinaryWriter extends Closeable {
        void addDoc(BytesRef v) throws IOException;

        default void flushData() throws IOException {}

        default void writeAddressMetadata(int minLength, int maxLength, int numDocsWithField) throws IOException {}

        @Override
        default void close() throws IOException {}
    }

    private final class DirectBinaryWriter implements BinaryWriter {
        final OffsetsAccumulator offsetsAccumulator;
        final BinaryDocValues values;

        private DirectBinaryWriter(OffsetsAccumulator offsetsAccumulator, BinaryDocValues values) {
            this.offsetsAccumulator = offsetsAccumulator;
            this.values = values;
        }

        @Override
        public void addDoc(BytesRef v) throws IOException {
            data.writeBytes(v.bytes, v.offset, v.length);
            if (offsetsAccumulator != null) {
                offsetsAccumulator.addDoc(v.length);
            }
        }

        @Override
        public void writeAddressMetadata(int minLength, int maxLength, int numDocsWithField) throws IOException {
            if (offsetsAccumulator != null) {
                // If optimized merging and minLength > maxLength
                offsetsAccumulator.build(meta, data);
            } else if (values != null) {
                if (maxLength > minLength) {
                    // If optimized merging and minLength > maxLength
                    long addressStart = data.getFilePointer();
                    meta.writeLong(addressStart);
                    meta.writeVInt(ES819TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT);

                    final DirectMonotonicWriter writer = DirectMonotonicWriter.getInstance(
                        meta,
                        data,
                        numDocsWithField + 1,
                        ES819TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT
                    );
                    long addr = 0;
                    writer.add(addr);
                    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                        addr += values.binaryValue().length;
                        writer.add(addr);
                    }
                    writer.finish();
                    meta.writeLong(data.getFilePointer() - addressStart);
                }
            }
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(offsetsAccumulator);
        }
    }

    private final class CompressedBinaryBlockWriter implements BinaryWriter {
        final Compressor compressor;

        final int[] docOffsets = new int[BLOCK_COUNT_THRESHOLD + 1]; // start for each doc plus start of doc that would be after last

        int uncompressedBlockLength = 0;
        int maxUncompressedBlockLength = 0;
        int numDocsInCurrentBlock = 0;

        byte[] block = BytesRef.EMPTY_BYTES;
        int totalChunks = 0;
        int maxNumDocsInAnyBlock = 0;

        final BlockMetadataAccumulator blockMetaAcc;

        CompressedBinaryBlockWriter(BinaryDVCompressionMode compressionMode) throws IOException {
            this.compressor = compressionMode.compressionMode().newCompressor();
            long blockAddressesStart = data.getFilePointer();
            this.blockMetaAcc = new BlockMetadataAccumulator(state.directory, state.context, data, blockAddressesStart);
        }

        @Override
        public void addDoc(BytesRef v) throws IOException {
            block = ArrayUtil.grow(block, uncompressedBlockLength + v.length);
            System.arraycopy(v.bytes, v.offset, block, uncompressedBlockLength, v.length);
            uncompressedBlockLength += v.length;

            numDocsInCurrentBlock++;
            docOffsets[numDocsInCurrentBlock] = uncompressedBlockLength;

            if (uncompressedBlockLength >= BLOCK_BYTES_THRESHOLD || numDocsInCurrentBlock >= BLOCK_COUNT_THRESHOLD) {
                flushData();
            }
        }

        @Override
        public void flushData() throws IOException {
            if (numDocsInCurrentBlock == 0) {
                return;
            }

            totalChunks++;
            long thisBlockStartPointer = data.getFilePointer();

            // Data can be compressed or uncompressed on a per-block granularity, though is currently always compressed.
            // In the future will leave data uncompressed if compression does not reduce storage.
            final boolean shouldCompress = enablePerBlockCompression;
            var header = new BinaryDVCompressionMode.BlockHeader(shouldCompress);
            data.writeByte(header.toByte());

            // write length of string data
            data.writeVInt(uncompressedBlockLength);

            maxUncompressedBlockLength = Math.max(maxUncompressedBlockLength, uncompressedBlockLength);
            maxNumDocsInAnyBlock = Math.max(maxNumDocsInAnyBlock, numDocsInCurrentBlock);

            compressOffsets(data, numDocsInCurrentBlock);

            if (shouldCompress) {
                compress(block, uncompressedBlockLength, data);
            } else {
                data.writeBytes(block, 0, uncompressedBlockLength);
            }

            long blockLenBytes = data.getFilePointer() - thisBlockStartPointer;
            blockMetaAcc.addDoc(numDocsInCurrentBlock, blockLenBytes);
            numDocsInCurrentBlock = uncompressedBlockLength = 0;
        }

        void compressOffsets(DataOutput output, int numDocsInCurrentBlock) throws IOException {
            int numOffsets = numDocsInCurrentBlock + 1;
            // delta encode
            for (int i = numOffsets - 1; i > 0; i--) {
                docOffsets[i] -= docOffsets[i - 1];
            }
            output.writeGroupVInts(docOffsets, numOffsets);
        }

        void compress(byte[] data, int uncompressedLength, DataOutput output) throws IOException {
            ByteBuffer inputBuffer = ByteBuffer.wrap(data, 0, uncompressedLength);
            ByteBuffersDataInput input = new ByteBuffersDataInput(List.of(inputBuffer));
            compressor.compress(input, output);
        }

        @Override
        public void writeAddressMetadata(int minLength, int maxLength, int numDocsWithField) throws IOException {
            if (totalChunks == 0) {
                return;
            }

            long dataAddressesStart = data.getFilePointer();
            meta.writeLong(dataAddressesStart);
            meta.writeVInt(totalChunks);
            meta.writeVInt(maxUncompressedBlockLength);
            meta.writeVInt(maxNumDocsInAnyBlock);
            meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);

            blockMetaAcc.build(meta, data);
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(blockMetaAcc);
        }
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        meta.writeInt(field.number);
        meta.writeByte(ES819TSDBDocValuesFormat.SORTED);
        doAddSortedField(field, valuesProducer, false);
    }

    @Override
    public void mergeSortedField(FieldInfo mergeFieldInfo, MergeState mergeState) throws IOException {
        var result = compatibleWithOptimizedMerge(enableOptimizedMerge, mergeState, mergeFieldInfo);
        if (result.supported()) {
            mergeSortedField(result, mergeFieldInfo, mergeState);
        } else {
            super.mergeSortedField(mergeFieldInfo, mergeState);
        }
    }

    private void doAddSortedField(FieldInfo field, DocValuesProducer valuesProducer, boolean addTypeByte) throws IOException {
        var producer = new TsdbDocValuesProducer(valuesProducer) {
            @Override
            public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
                SortedDocValues sorted = valuesProducer.getSorted(field);
                NumericDocValues sortedOrds = new NumericDocValues() {
                    @Override
                    public long longValue() throws IOException {
                        return sorted.ordValue();
                    }

                    @Override
                    public boolean advanceExact(int target) throws IOException {
                        return sorted.advanceExact(target);
                    }

                    @Override
                    public int docID() {
                        return sorted.docID();
                    }

                    @Override
                    public int nextDoc() throws IOException {
                        return sorted.nextDoc();
                    }

                    @Override
                    public int advance(int target) throws IOException {
                        return sorted.advance(target);
                    }

                    @Override
                    public long cost() {
                        return sorted.cost();
                    }
                };
                return DocValues.singleton(sortedOrds);
            }
        };
        if (field.docValuesSkipIndexType() != DocValuesSkipIndexType.NONE) {
            writeSkipIndex(field, producer);
        }
        if (addTypeByte) {
            meta.writeByte((byte) 0); // multiValued (0 = singleValued)
        }
        SortedDocValues sorted = valuesProducer.getSorted(field);
        int maxOrd = sorted.getValueCount();
        writeField(field, producer, maxOrd, null);
        addTermsDict(DocValues.singleton(valuesProducer.getSorted(field)));
    }

    private static final int FSST_BULK_BUFFER_SIZE = 128 * 1024;

    private void addTermsDict(SortedSetDocValues values) throws IOException {
        final long size = values.getValueCount();
        meta.writeVLong(size);

        // First pass: sample terms for FSST training
        ReservoirSampler sampler = new ReservoirSampler(FSST_BULK_BUFFER_SIZE);
        int maxLength = 0;
        {
            TermsEnum sampleIterator = values.termsEnum();
            for (BytesRef term = sampleIterator.next(); term != null; term = sampleIterator.next()) {
                sampler.processLine(term.bytes, term.offset, term.length);
                maxLength = Math.max(maxLength, term.length);
            }
        }

        FSST.SymbolTable symbolTable = FSST.SymbolTable.buildSymbolTable(sampler.getSample());
        byte[] symbolTableBytes = symbolTable.exportToBytes();

        // Write meta that we know upfront: blockShift
        meta.writeInt(DIRECT_MONOTONIC_BLOCK_SHIFT);

        // Use temp buffers for DMW metadata to avoid interleaving on the shared meta stream.
        // Both writers flush per-block metadata during add() calls, so writing to the real meta
        // directly would interleave their blocks. We buffer each separately, then copy in order.
        ByteBuffersDataOutput suffixOffsetsMetaBuffer = new ByteBuffersDataOutput();
        ByteBuffersIndexOutput suffixOffsetsMetaOutput = new ByteBuffersIndexOutput(suffixOffsetsMetaBuffer, "temp", "temp");
        ByteBuffersDataOutput suffixOffsetsData = new ByteBuffersDataOutput();
        ByteBuffersIndexOutput suffixOffsetsOutput = new ByteBuffersIndexOutput(suffixOffsetsData, "temp", "temp");
        DirectMonotonicWriter suffixOffsetsWriter = DirectMonotonicWriter.getInstance(
            suffixOffsetsMetaOutput,
            suffixOffsetsOutput,
            Math.max(size + 1, 1),
            DIRECT_MONOTONIC_BLOCK_SHIFT
        );

        ByteBuffersDataOutput prefixIdsMetaBuffer = new ByteBuffersDataOutput();
        ByteBuffersIndexOutput prefixIdsMetaOutput = new ByteBuffersIndexOutput(prefixIdsMetaBuffer, "temp", "temp");
        ByteBuffersDataOutput prefixIdsData = new ByteBuffersDataOutput();
        ByteBuffersIndexOutput prefixIdsOutput = new ByteBuffersIndexOutput(prefixIdsData, "temp", "temp");
        DirectMonotonicWriter prefixIdsWriter = DirectMonotonicWriter.getInstance(
            prefixIdsMetaOutput,
            prefixIdsOutput,
            Math.max(size, 1),
            DIRECT_MONOTONIC_BLOCK_SHIFT
        );

        // Write FSST symbol table
        long symbolTableStart = data.getFilePointer();
        data.writeVInt(symbolTableBytes.length);
        data.writeBytes(symbolTableBytes, 0, symbolTableBytes.length);
        long symbolTableEnd = data.getFilePointer();

        // Second pass: iterate terms with one-term lookahead
        TermsEnum iterator = values.termsEnum();

        // Suffix batch buffers (same pattern as old flat approach)
        byte[] batchBytes = new byte[FSST_BULK_BUFFER_SIZE + maxLength];
        int batchPos = 0;
        int batchCount = 0;
        int[] batchInOffsets = new int[1024 + 1];
        byte[] fsstOutBuf = new byte[batchBytes.length * 2 + 8];
        int[] batchOutOffsets = new int[batchInOffsets.length];
        long suffixCompressedPos = 0;
        long suffixBatchBase = 0;

        // Prefix accumulator
        byte[] prefixAccumBuf = new byte[FSST_BULK_BUFFER_SIZE];
        int[] prefixAccumStarts = new int[1024];
        int prefixAccumPos = 0;
        int numPrefixes = 0;

        // Current prefix tracking
        byte[] currentPrefixBytes = new byte[maxLength + 1];
        int currentPrefixLen = 0;

        BytesRefBuilder prevTerm = new BytesRefBuilder();
        BytesRefBuilder currentTerm = new BytesRefBuilder();

        long suffixDataStart = data.getFilePointer();

        BytesRef rawTerm = iterator.next();
        if (rawTerm != null) {
            currentTerm.copyBytes(rawTerm);
            rawTerm = iterator.next();
        }

        for (long ord = 0; ord < size; ord++) {
            BytesRef cur = currentTerm.get();
            BytesRef next = rawTerm;
            int prefixLen;
            int prefixId;

            if (ord == 0) {
                if (size == 1) {
                    prefixLen = cur.length;
                } else {
                    prefixLen = computeLCP(cur, next);
                }
                prefixAccumStarts[numPrefixes] = prefixAccumPos;
                System.arraycopy(cur.bytes, cur.offset, prefixAccumBuf, prefixAccumPos, prefixLen);
                prefixAccumPos += prefixLen;
                System.arraycopy(cur.bytes, cur.offset, currentPrefixBytes, 0, prefixLen);
                currentPrefixLen = prefixLen;
                prefixId = 0;
                numPrefixes = 1;
            } else {
                int backwardLcp = computeLCP(prevTerm.get(), cur);
                boolean matchesCurrent = (backwardLcp == currentPrefixLen)
                    && Arrays.equals(cur.bytes, cur.offset, cur.offset + currentPrefixLen, currentPrefixBytes, 0, currentPrefixLen);

                if (matchesCurrent) {
                    prefixId = numPrefixes - 1;
                    prefixLen = currentPrefixLen;
                } else {
                    int chosenLen = backwardLcp;
                    if (next != null) {
                        int forwardLcp = computeLCP(cur, next);
                        chosenLen = Math.max(backwardLcp, forwardLcp);
                    }
                    prefixLen = chosenLen;
                    if (prefixAccumPos + prefixLen > prefixAccumBuf.length) {
                        prefixAccumBuf = ArrayUtil.grow(prefixAccumBuf, prefixAccumPos + prefixLen);
                    }
                    if (numPrefixes >= prefixAccumStarts.length) {
                        prefixAccumStarts = ArrayUtil.grow(prefixAccumStarts, numPrefixes + 1);
                    }
                    prefixAccumStarts[numPrefixes] = prefixAccumPos;
                    System.arraycopy(cur.bytes, cur.offset, prefixAccumBuf, prefixAccumPos, prefixLen);
                    prefixAccumPos += prefixLen;
                    if (prefixLen > currentPrefixBytes.length) {
                        currentPrefixBytes = new byte[prefixLen];
                    }
                    System.arraycopy(cur.bytes, cur.offset, currentPrefixBytes, 0, prefixLen);
                    currentPrefixLen = prefixLen;
                    prefixId = numPrefixes;
                    numPrefixes++;
                }
            }

            prefixIdsWriter.add(prefixId);

            // Add suffix to batch
            int suffixLen = cur.length - prefixLen;
            if (batchCount > 0 && batchPos + suffixLen > FSST_BULK_BUFFER_SIZE) {
                suffixCompressedPos = flushSuffixBatch(
                    symbolTable,
                    batchBytes,
                    batchInOffsets,
                    batchCount,
                    batchPos,
                    fsstOutBuf,
                    batchOutOffsets,
                    suffixOffsetsWriter,
                    suffixBatchBase
                );
                batchPos = 0;
                batchCount = 0;
                suffixBatchBase = suffixCompressedPos;
            }
            if (batchCount + 1 >= batchInOffsets.length) {
                batchInOffsets = ArrayUtil.grow(batchInOffsets, batchCount + 2);
                batchOutOffsets = new int[batchInOffsets.length];
            }
            batchInOffsets[batchCount] = batchPos;
            System.arraycopy(cur.bytes, cur.offset + prefixLen, batchBytes, batchPos, suffixLen);
            batchPos += suffixLen;
            batchCount++;

            prevTerm.copyBytes(cur);
            if (rawTerm != null) {
                currentTerm.copyBytes(rawTerm);
                rawTerm = iterator.next();
            }
        }

        // Flush remaining suffix batch
        if (batchCount > 0) {
            suffixCompressedPos = flushSuffixBatch(
                symbolTable,
                batchBytes,
                batchInOffsets,
                batchCount,
                batchPos,
                fsstOutBuf,
                batchOutOffsets,
                suffixOffsetsWriter,
                suffixBatchBase
            );
        }
        suffixOffsetsWriter.add(suffixCompressedPos);
        if (size == 0) {
            prefixIdsWriter.add(0);
        }
        suffixOffsetsWriter.finish();
        prefixIdsWriter.finish();

        // Write DMW metadata to the real meta stream in order (suffix offsets first, then prefix IDs)
        suffixOffsetsMetaBuffer.copyTo(meta);
        prefixIdsMetaBuffer.copyTo(meta);

        long suffixDataEnd = data.getFilePointer();

        // Compress and write all accumulated prefix data
        if (numPrefixes >= prefixAccumStarts.length) {
            prefixAccumStarts = ArrayUtil.grow(prefixAccumStarts, numPrefixes + 1);
        }
        prefixAccumStarts[numPrefixes] = prefixAccumPos;

        // Write numPrefixes and prefix offsets DMW meta to meta (deferred until now)
        meta.writeVLong(numPrefixes);

        ByteBuffersDataOutput prefixOffsetsData = new ByteBuffersDataOutput();
        ByteBuffersIndexOutput prefixOffsetsOutput = new ByteBuffersIndexOutput(prefixOffsetsData, "temp", "temp");
        DirectMonotonicWriter prefixOffsetsWriter = DirectMonotonicWriter.getInstance(
            meta,
            prefixOffsetsOutput,
            numPrefixes + 1,
            DIRECT_MONOTONIC_BLOCK_SHIFT
        );

        long prefixDataStart = data.getFilePointer();
        long prefixGlobalPos = 0;
        if (numPrefixes > 0) {
            int prefixBatchStart = 0;
            while (prefixBatchStart < numPrefixes) {
                int prefixBatchEnd = prefixBatchStart;
                int prefixBatchBytes = 0;
                while (prefixBatchEnd < numPrefixes) {
                    int pLen = prefixAccumStarts[prefixBatchEnd + 1] - prefixAccumStarts[prefixBatchEnd];
                    if (prefixBatchBytes > 0 && prefixBatchBytes + pLen > FSST_BULK_BUFFER_SIZE) break;
                    prefixBatchBytes += pLen;
                    prefixBatchEnd++;
                }
                int pCount = prefixBatchEnd - prefixBatchStart;
                int[] pInputOffsets = new int[pCount + 1];
                for (int i = 0; i <= pCount; i++) {
                    pInputOffsets[i] = prefixAccumStarts[prefixBatchStart + i];
                }
                int pTotalBytes = pInputOffsets[pCount] - pInputOffsets[0];

                int pWorst = 2 * pTotalBytes + 8 * pCount;
                if (fsstOutBuf.length < pWorst) {
                    fsstOutBuf = new byte[ArrayUtil.oversize(pWorst, 1)];
                }
                int[] pOutOffsets = new int[pCount + 1];

                symbolTable.compressBulk(pCount, prefixAccumBuf, pInputOffsets, fsstOutBuf, pOutOffsets);

                int compStart = pOutOffsets[0];
                int compTotal = pOutOffsets[pCount] - compStart;
                if (compTotal > 0) {
                    data.writeBytes(fsstOutBuf, compStart, compTotal);
                }
                for (int i = 0; i < pCount; i++) {
                    prefixOffsetsWriter.add(prefixGlobalPos + (pOutOffsets[i] - compStart));
                }
                prefixGlobalPos += compTotal;
                prefixBatchStart = prefixBatchEnd;
            }
        }
        prefixOffsetsWriter.add(prefixGlobalPos);
        prefixOffsetsWriter.finish();

        long prefixDataEnd = data.getFilePointer();

        // Write DMW data sections to data file
        long suffixOffsetsDataStart = data.getFilePointer();
        suffixOffsetsData.copyTo(data);
        long suffixOffsetsDataEnd = data.getFilePointer();

        long prefixIdsDataStart = data.getFilePointer();
        prefixIdsData.copyTo(data);
        long prefixIdsDataEnd = data.getFilePointer();

        long prefixOffsetsDataStart = data.getFilePointer();
        prefixOffsetsData.copyTo(data);
        long prefixOffsetsDataEnd = data.getFilePointer();

        // Write remaining metadata
        meta.writeInt(maxLength);
        meta.writeLong(symbolTableStart);
        meta.writeLong(symbolTableEnd - symbolTableStart);
        meta.writeLong(suffixDataStart);
        meta.writeLong(suffixDataEnd - suffixDataStart);
        meta.writeLong(prefixDataStart);
        meta.writeLong(prefixDataEnd - prefixDataStart);
        meta.writeLong(suffixOffsetsDataStart);
        meta.writeLong(suffixOffsetsDataEnd - suffixOffsetsDataStart);
        meta.writeLong(prefixIdsDataStart);
        meta.writeLong(prefixIdsDataEnd - prefixIdsDataStart);
        meta.writeLong(prefixOffsetsDataStart);
        meta.writeLong(prefixOffsetsDataEnd - prefixOffsetsDataStart);

        writeTermsIndex(values);
    }

    private long flushSuffixBatch(
        FSST.SymbolTable symbolTable,
        byte[] batchBytes,
        int[] batchInOffsets,
        int batchCount,
        int batchPos,
        byte[] fsstOutBuf,
        int[] batchOutOffsets,
        DirectMonotonicWriter suffixOffsetsWriter,
        long batchBaseCompressedPos
    ) throws IOException {
        batchInOffsets[batchCount] = batchPos;

        int worstCase = 2 * batchPos + 8 * batchCount;
        if (fsstOutBuf.length < worstCase) {
            fsstOutBuf = new byte[worstCase];
        }
        if (batchOutOffsets.length < batchCount + 1) {
            batchOutOffsets = new int[batchCount + 1];
        }

        symbolTable.compressBulk(batchCount, batchBytes, batchInOffsets, fsstOutBuf, batchOutOffsets);

        int compressedStart = batchOutOffsets[0];
        int compressedTotal = batchOutOffsets[batchCount] - compressedStart;
        if (compressedTotal > 0) {
            data.writeBytes(fsstOutBuf, compressedStart, compressedTotal);
        }

        for (int i = 0; i < batchCount; i++) {
            suffixOffsetsWriter.add(batchBaseCompressedPos + (batchOutOffsets[i] - compressedStart));
        }

        return batchBaseCompressedPos + compressedTotal;
    }

    private static int computeLCP(BytesRef a, BytesRef b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            if (a.bytes[a.offset + i] != b.bytes[b.offset + i]) {
                return i;
            }
        }
        return len;
    }

    private void writeTermsIndex(SortedSetDocValues values) throws IOException {
        final long size = values.getValueCount();
        meta.writeInt(ES819TSDBDocValuesFormat.TERMS_DICT_REVERSE_INDEX_SHIFT);
        long start = data.getFilePointer();

        long numBlocks = 1L + ((size + ES819TSDBDocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK)
            >>> ES819TSDBDocValuesFormat.TERMS_DICT_REVERSE_INDEX_SHIFT);
        ByteBuffersDataOutput addressBuffer = new ByteBuffersDataOutput();
        DirectMonotonicWriter writer;
        try (ByteBuffersIndexOutput addressOutput = new ByteBuffersIndexOutput(addressBuffer, "temp", "temp")) {
            writer = DirectMonotonicWriter.getInstance(meta, addressOutput, numBlocks, DIRECT_MONOTONIC_BLOCK_SHIFT);
            TermsEnum iterator = values.termsEnum();
            BytesRefBuilder previous = new BytesRefBuilder();
            long offset = 0;
            long ord = 0;
            for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
                if ((ord & ES819TSDBDocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK) == 0) {
                    writer.add(offset);
                    final int sortKeyLength;
                    if (ord == 0) {
                        // no previous term: no bytes to write
                        sortKeyLength = 0;
                    } else {
                        sortKeyLength = StringHelper.sortKeyLength(previous.get(), term);
                    }
                    offset += sortKeyLength;
                    data.writeBytes(term.bytes, term.offset, sortKeyLength);
                } else if ((ord
                    & ES819TSDBDocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK) == ES819TSDBDocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK) {
                        previous.copyBytes(term);
                    }
                ++ord;
            }
            writer.add(offset);
            writer.finish();
            meta.writeLong(start);
            meta.writeLong(data.getFilePointer() - start);
            start = data.getFilePointer();
            addressBuffer.copyTo(data);
            meta.writeLong(start);
            meta.writeLong(data.getFilePointer() - start);
        }
    }

    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        meta.writeInt(field.number);
        meta.writeByte(ES819TSDBDocValuesFormat.SORTED_NUMERIC);
        writeSortedNumericField(field, new TsdbDocValuesProducer(valuesProducer), -1);
    }

    private void writeSortedNumericField(FieldInfo field, TsdbDocValuesProducer valuesProducer, long maxOrd) throws IOException {
        if (field.docValuesSkipIndexType() != DocValuesSkipIndexType.NONE) {
            writeSkipIndex(field, valuesProducer);
        }
        if (maxOrd > -1) {
            meta.writeByte((byte) 1); // multiValued (1 = multiValued)
        }

        if (valuesProducer.mergeStats.supported()) {
            int numDocsWithField = valuesProducer.mergeStats.sumNumDocsWithField();
            long numValues = valuesProducer.mergeStats.sumNumValues();
            if (numDocsWithField == numValues) {
                writeField(field, valuesProducer, maxOrd, null);
            } else {
                assert numValues > numDocsWithField;
                try (var accumulator = new OffsetsAccumulator(dir, context, data, numDocsWithField)) {
                    writeField(field, valuesProducer, maxOrd, accumulator);
                    accumulator.build(meta, data);
                }
            }
        } else {
            long[] stats = writeField(field, valuesProducer, maxOrd, null);
            int numDocsWithField = Math.toIntExact(stats[0]);
            long numValues = stats[1];
            assert numValues >= numDocsWithField;

            if (numValues > numDocsWithField) {
                long start = data.getFilePointer();
                meta.writeLong(start);
                meta.writeVInt(ES819TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT);

                final DirectMonotonicWriter addressesWriter = DirectMonotonicWriter.getInstance(
                    meta,
                    data,
                    numDocsWithField + 1L,
                    ES819TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT
                );
                long addr = 0;
                addressesWriter.add(addr);
                SortedNumericDocValues values = valuesProducer.getSortedNumeric(field);
                for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                    addr += values.docValueCount();
                    addressesWriter.add(addr);
                }
                addressesWriter.finish();
                meta.writeLong(data.getFilePointer() - start);
            }
        }
    }

    @Override
    public void mergeSortedNumericField(FieldInfo mergeFieldInfo, MergeState mergeState) throws IOException {
        var result = compatibleWithOptimizedMerge(enableOptimizedMerge, mergeState, mergeFieldInfo);
        if (result.supported()) {
            mergeSortedNumericField(result, mergeFieldInfo, mergeState);
        } else {
            super.mergeSortedNumericField(mergeFieldInfo, mergeState);
        }
    }

    private static boolean isSingleValued(FieldInfo field, TsdbDocValuesProducer producer) throws IOException {
        if (producer.mergeStats.supported()) {
            return producer.mergeStats.sumNumValues() == producer.mergeStats.sumNumDocsWithField();
        }

        var values = producer.getSortedSet(field);
        if (DocValues.unwrapSingleton(values) != null) {
            return true;
        }

        assert values.docID() == -1;
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            int docValueCount = values.docValueCount();
            assert docValueCount > 0;
            if (docValueCount > 1) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void mergeSortedSetField(FieldInfo mergeFieldInfo, MergeState mergeState) throws IOException {
        var result = compatibleWithOptimizedMerge(enableOptimizedMerge, mergeState, mergeFieldInfo);
        if (result.supported()) {
            mergeSortedSetField(result, mergeFieldInfo, mergeState);
        } else {
            super.mergeSortedSetField(mergeFieldInfo, mergeState);
        }
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        meta.writeInt(field.number);
        meta.writeByte(SORTED_SET);

        if (isSingleValued(field, new TsdbDocValuesProducer(valuesProducer))) {
            doAddSortedField(field, new TsdbDocValuesProducer(valuesProducer) {
                @Override
                public SortedDocValues getSorted(FieldInfo field) throws IOException {
                    return SortedSetSelector.wrap(valuesProducer.getSortedSet(field), SortedSetSelector.Type.MIN);
                }
            }, true);
            return;
        }

        SortedSetDocValues values = valuesProducer.getSortedSet(field);
        long maxOrd = values.getValueCount();
        writeSortedNumericField(field, new TsdbDocValuesProducer(valuesProducer) {
            @Override
            public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
                SortedSetDocValues values = valuesProducer.getSortedSet(field);
                return new SortedNumericDocValues() {

                    long[] ords = LongsRef.EMPTY_LONGS;
                    int i, docValueCount;

                    @Override
                    public long nextValue() {
                        return ords[i++];
                    }

                    @Override
                    public int docValueCount() {
                        return docValueCount;
                    }

                    @Override
                    public boolean advanceExact(int target) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public int docID() {
                        return values.docID();
                    }

                    @Override
                    public int nextDoc() throws IOException {
                        int doc = values.nextDoc();
                        if (doc != NO_MORE_DOCS) {
                            docValueCount = values.docValueCount();
                            ords = ArrayUtil.grow(ords, docValueCount);
                            for (int j = 0; j < docValueCount; j++) {
                                ords[j] = values.nextOrd();
                            }
                            i = 0;
                        }
                        return doc;
                    }

                    @Override
                    public int advance(int target) throws IOException {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public long cost() {
                        return values.cost();
                    }
                };
            }
        }, maxOrd);

        addTermsDict(valuesProducer.getSortedSet(field));
    }

    @Override
    public void close() throws IOException {
        boolean success = false;
        try {
            if (meta != null) {
                meta.writeInt(-1); // write EOF marker
                CodecUtil.writeFooter(meta); // write checksum
            }
            if (data != null) {
                CodecUtil.writeFooter(data); // write checksum
            }
            success = true;
        } finally {
            if (success) {
                IOUtils.close(data, meta);
            } else {
                IOUtils.closeWhileHandlingException(data, meta);
            }
            meta = data = null;
        }
    }

    private static class SkipAccumulator {
        int minDocID;
        int maxDocID;
        int docCount;
        long minValue;
        long maxValue;

        SkipAccumulator(int docID) {
            minDocID = docID;
            minValue = Long.MAX_VALUE;
            maxValue = Long.MIN_VALUE;
            docCount = 0;
        }

        boolean isDone(int skipIndexIntervalSize, int valueCount, long nextValue, int nextDoc) {
            if (docCount < skipIndexIntervalSize) {
                return false;
            }
            // Once we reach the interval size, we will keep accepting documents if
            // - next doc value is not a multi-value
            // - current accumulator only contains a single value and next value is the same value
            // - the accumulator is dense and the next doc keeps the density (no gaps)
            return valueCount > 1 || minValue != maxValue || minValue != nextValue || docCount != nextDoc - minDocID;
        }

        void accumulate(long value) {
            minValue = Math.min(minValue, value);
            maxValue = Math.max(maxValue, value);
        }

        void accumulate(SkipAccumulator other) {
            assert minDocID <= other.minDocID && maxDocID < other.maxDocID;
            maxDocID = other.maxDocID;
            minValue = Math.min(minValue, other.minValue);
            maxValue = Math.max(maxValue, other.maxValue);
            docCount += other.docCount;
        }

        void nextDoc(int docID) {
            maxDocID = docID;
            ++docCount;
        }

        public static SkipAccumulator merge(List<SkipAccumulator> list, int index, int length) {
            SkipAccumulator acc = new SkipAccumulator(list.get(index).minDocID);
            for (int i = 0; i < length; i++) {
                acc.accumulate(list.get(index + i));
            }
            return acc;
        }
    }

    private void writeSkipIndex(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        assert field.docValuesSkipIndexType() != DocValuesSkipIndexType.NONE;
        final long start = data.getFilePointer();
        final SortedNumericDocValues values = valuesProducer.getSortedNumeric(field);
        long globalMaxValue = Long.MIN_VALUE;
        long globalMinValue = Long.MAX_VALUE;
        int globalDocCount = 0;
        int maxDocId = -1;
        final List<SkipAccumulator> accumulators = new ArrayList<>();
        SkipAccumulator accumulator = null;
        final int maxAccumulators = 1 << (SKIP_INDEX_LEVEL_SHIFT * (SKIP_INDEX_MAX_LEVEL - 1));
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            final long firstValue = values.nextValue();
            if (accumulator != null && accumulator.isDone(skipIndexIntervalSize, values.docValueCount(), firstValue, doc)) {
                globalMaxValue = Math.max(globalMaxValue, accumulator.maxValue);
                globalMinValue = Math.min(globalMinValue, accumulator.minValue);
                globalDocCount += accumulator.docCount;
                maxDocId = accumulator.maxDocID;
                accumulator = null;
                if (accumulators.size() == maxAccumulators) {
                    writeLevels(accumulators);
                    accumulators.clear();
                }
            }
            if (accumulator == null) {
                accumulator = new SkipAccumulator(doc);
                accumulators.add(accumulator);
            }
            accumulator.nextDoc(doc);
            accumulator.accumulate(firstValue);
            for (int i = 1, end = values.docValueCount(); i < end; ++i) {
                accumulator.accumulate(values.nextValue());
            }
        }

        if (accumulators.isEmpty() == false) {
            globalMaxValue = Math.max(globalMaxValue, accumulator.maxValue);
            globalMinValue = Math.min(globalMinValue, accumulator.minValue);
            globalDocCount += accumulator.docCount;
            maxDocId = accumulator.maxDocID;
            writeLevels(accumulators);
        }
        meta.writeLong(start); // record the start in meta
        meta.writeLong(data.getFilePointer() - start); // record the length
        assert globalDocCount == 0 || globalMaxValue >= globalMinValue;
        meta.writeLong(globalMaxValue);
        meta.writeLong(globalMinValue);
        assert globalDocCount <= maxDocId + 1;
        meta.writeInt(globalDocCount);
        meta.writeInt(maxDocId);
    }

    private void writeLevels(List<SkipAccumulator> accumulators) throws IOException {
        final List<List<SkipAccumulator>> accumulatorsLevels = new ArrayList<>(SKIP_INDEX_MAX_LEVEL);
        accumulatorsLevels.add(accumulators);
        for (int i = 0; i < SKIP_INDEX_MAX_LEVEL - 1; i++) {
            accumulatorsLevels.add(buildLevel(accumulatorsLevels.get(i)));
        }
        int totalAccumulators = accumulators.size();
        for (int index = 0; index < totalAccumulators; index++) {
            // compute how many levels we need to write for the current accumulator
            final int levels = getLevels(index, totalAccumulators);
            // write the number of levels
            data.writeByte((byte) levels);
            // write intervals in reverse order. This is done so we don't
            // need to read all of them in case of slipping
            for (int level = levels - 1; level >= 0; level--) {
                final SkipAccumulator accumulator = accumulatorsLevels.get(level).get(index >> (SKIP_INDEX_LEVEL_SHIFT * level));
                data.writeInt(accumulator.maxDocID);
                data.writeInt(accumulator.minDocID);
                data.writeLong(accumulator.maxValue);
                data.writeLong(accumulator.minValue);
                data.writeInt(accumulator.docCount);
            }
        }
    }

    private static List<SkipAccumulator> buildLevel(List<SkipAccumulator> accumulators) {
        final int levelSize = 1 << SKIP_INDEX_LEVEL_SHIFT;
        final List<SkipAccumulator> collector = new ArrayList<>();
        for (int i = 0; i < accumulators.size() - levelSize + 1; i += levelSize) {
            collector.add(SkipAccumulator.merge(accumulators, i, levelSize));
        }
        return collector;
    }

    private static int getLevels(int index, int size) {
        if (Integer.numberOfTrailingZeros(index) >= SKIP_INDEX_LEVEL_SHIFT) {
            // TODO: can we do it in constant time rather than linearly with SKIP_INDEX_MAX_LEVEL?
            final int left = size - index;
            for (int level = SKIP_INDEX_MAX_LEVEL - 1; level > 0; level--) {
                final int numberIntervals = 1 << (SKIP_INDEX_LEVEL_SHIFT * level);
                if (left >= numberIntervals && index % numberIntervals == 0) {
                    return level + 1;
                }
            }
        }
        return 1;
    }

}
