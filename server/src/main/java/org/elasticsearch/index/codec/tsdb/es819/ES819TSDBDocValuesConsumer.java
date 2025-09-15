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
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
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
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.compress.LZ4;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesEncoder;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.index.codec.tsdb.es819.DocValuesConsumerUtil.compatibleWithOptimizedMerge;
import static org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat.SORTED_SET;

final class ES819TSDBDocValuesConsumer extends XDocValuesConsumer {

    final Directory dir;
    final IOContext context;
    IndexOutput data, meta;
    final int maxDoc;
    private byte[] termsDictBuffer;
    final boolean enableOptimizedMerge;

    ES819TSDBDocValuesConsumer(
        SegmentWriteState state,
        boolean enableOptimizedMerge,
        String dataCodec,
        String dataExtension,
        String metaCodec,
        String metaExtension
    ) throws IOException {
        this.termsDictBuffer = new byte[1 << 14];
        this.dir = state.directory;
        this.context = state.context;
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
            maxDoc = state.segmentInfo.maxDoc();
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

        writeField(field, producer, -1, null);
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
                // Special case for maxOrd of 1, signal -1 that no blocks will be written
                meta.writeInt(maxOrd != 1 ? ES819TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT : -1);
                final ByteBuffersDataOutput indexOut = new ByteBuffersDataOutput();
                final DirectMonotonicWriter indexWriter = DirectMonotonicWriter.getInstance(
                    meta,
                    new ByteBuffersIndexOutput(indexOut, "temp-dv-index", "temp-dv-index"),
                    1L + ((numValues - 1) >>> ES819TSDBDocValuesFormat.NUMERIC_BLOCK_SHIFT),
                    ES819TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT
                );

                final long valuesDataOffset = data.getFilePointer();
                // Special case for maxOrd of 1, skip writing the blocks
                if (maxOrd != 1) {
                    final long[] buffer = new long[ES819TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE];
                    int bufferSize = 0;
                    final TSDBDocValuesEncoder encoder = new TSDBDocValuesEncoder(ES819TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE);
                    values = valuesProducer.getSortedNumeric(field);
                    final int bitsPerOrd = maxOrd >= 0 ? PackedInts.bitsRequired(maxOrd - 1) : -1;
                    if (enableOptimizedMerge && numDocsWithValue < maxDoc) {
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
                            if (bufferSize == ES819TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE) {
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
                        Arrays.fill(buffer, bufferSize, ES819TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE, 0L);
                        if (maxOrd >= 0) {
                            encoder.encodeOrdinals(buffer, data, bitsPerOrd);
                        } else {
                            encoder.encode(buffer, data);
                        }
                    }
                }

                final long valuesDataLength = data.getFilePointer() - valuesDataOffset;
                if (maxOrd != 1) {
                    // Special case for maxOrd of 1, indexWriter isn't really used, so no need to invoke finish() method.
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

        if (valuesProducer instanceof TsdbDocValuesProducer tsdbValuesProducer && tsdbValuesProducer.mergeStats.supported()) {
            final int numDocsWithField = tsdbValuesProducer.mergeStats.sumNumDocsWithField();
            final int minLength = tsdbValuesProducer.mergeStats.minLength();
            final int maxLength = tsdbValuesProducer.mergeStats.maxLength();

            assert numDocsWithField <= maxDoc;

            BinaryDocValues values = valuesProducer.getBinary(field);
            long start = data.getFilePointer();
            meta.writeLong(start); // dataOffset

            OffsetsAccumulator offsetsAccumulator = null;
            DISIAccumulator disiAccumulator = null;
            try {
                if (numDocsWithField > 0 && numDocsWithField < maxDoc) {
                    disiAccumulator = new DISIAccumulator(dir, context, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
                }

                assert maxLength >= minLength;
                if (maxLength > minLength) {
                    offsetsAccumulator = new OffsetsAccumulator(dir, context, data, numDocsWithField);
                }

                for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                    BytesRef v = values.binaryValue();
                    data.writeBytes(v.bytes, v.offset, v.length);
                    if (disiAccumulator != null) {
                        disiAccumulator.addDocId(doc);
                    }
                    if (offsetsAccumulator != null) {
                        offsetsAccumulator.addDoc(v.length);
                    }
                }
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
                if (offsetsAccumulator != null) {
                    offsetsAccumulator.build(meta, data);
                }
            } finally {
                IOUtils.close(disiAccumulator, offsetsAccumulator);
            }
        } else {
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
                data.writeBytes(v.bytes, v.offset, v.length);
                minLength = Math.min(length, minLength);
                maxLength = Math.max(length, maxLength);
            }
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
            if (maxLength > minLength) {
                start = data.getFilePointer();
                meta.writeLong(start);
                meta.writeVInt(ES819TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT);

                final DirectMonotonicWriter writer = DirectMonotonicWriter.getInstance(
                    meta,
                    data,
                    numDocsWithField + 1,
                    ES819TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT
                );
                long addr = 0;
                writer.add(addr);
                values = valuesProducer.getBinary(field);
                for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                    addr += values.binaryValue().length;
                    writer.add(addr);
                }
                writer.finish();
                meta.writeLong(data.getFilePointer() - start);
            }
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
        if (addTypeByte) {
            meta.writeByte((byte) 0); // multiValued (0 = singleValued)
        }
        SortedDocValues sorted = valuesProducer.getSorted(field);
        int maxOrd = sorted.getValueCount();
        writeField(field, producer, maxOrd, null);
        addTermsDict(DocValues.singleton(valuesProducer.getSorted(field)));
    }

    private void addTermsDict(SortedSetDocValues values) throws IOException {
        final long size = values.getValueCount();
        meta.writeVLong(size);

        int blockMask = ES819TSDBDocValuesFormat.TERMS_DICT_BLOCK_LZ4_MASK;
        int shift = ES819TSDBDocValuesFormat.TERMS_DICT_BLOCK_LZ4_SHIFT;

        meta.writeInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
        ByteBuffersDataOutput addressBuffer = new ByteBuffersDataOutput();
        ByteBuffersIndexOutput addressOutput = new ByteBuffersIndexOutput(addressBuffer, "temp", "temp");
        long numBlocks = (size + blockMask) >>> shift;
        DirectMonotonicWriter writer = DirectMonotonicWriter.getInstance(meta, addressOutput, numBlocks, DIRECT_MONOTONIC_BLOCK_SHIFT);

        BytesRefBuilder previous = new BytesRefBuilder();
        long ord = 0;
        long start = data.getFilePointer();
        int maxLength = 0, maxBlockLength = 0;
        TermsEnum iterator = values.termsEnum();

        LZ4.FastCompressionHashTable ht = new LZ4.FastCompressionHashTable();
        ByteArrayDataOutput bufferedOutput = new ByteArrayDataOutput(termsDictBuffer);
        int dictLength = 0;

        for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
            if ((ord & blockMask) == 0) {
                if (ord != 0) {
                    // flush the previous block
                    final int uncompressedLength = compressAndGetTermsDictBlockLength(bufferedOutput, dictLength, ht);
                    maxBlockLength = Math.max(maxBlockLength, uncompressedLength);
                    bufferedOutput.reset(termsDictBuffer);
                }

                writer.add(data.getFilePointer() - start);
                // Write the first term both to the index output, and to the buffer where we'll use it as a
                // dictionary for compression
                data.writeVInt(term.length);
                data.writeBytes(term.bytes, term.offset, term.length);
                bufferedOutput = maybeGrowBuffer(bufferedOutput, term.length);
                bufferedOutput.writeBytes(term.bytes, term.offset, term.length);
                dictLength = term.length;
            } else {
                final int prefixLength = StringHelper.bytesDifference(previous.get(), term);
                final int suffixLength = term.length - prefixLength;
                assert suffixLength > 0; // terms are unique
                // Will write (suffixLength + 1 byte + 2 vint) bytes. Grow the buffer in need.
                bufferedOutput = maybeGrowBuffer(bufferedOutput, suffixLength + 11);
                bufferedOutput.writeByte((byte) (Math.min(prefixLength, 15) | (Math.min(15, suffixLength - 1) << 4)));
                if (prefixLength >= 15) {
                    bufferedOutput.writeVInt(prefixLength - 15);
                }
                if (suffixLength >= 16) {
                    bufferedOutput.writeVInt(suffixLength - 16);
                }
                bufferedOutput.writeBytes(term.bytes, term.offset + prefixLength, suffixLength);
            }
            maxLength = Math.max(maxLength, term.length);
            previous.copyBytes(term);
            ++ord;
        }
        // Compress and write out the last block
        if (bufferedOutput.getPosition() > dictLength) {
            final int uncompressedLength = compressAndGetTermsDictBlockLength(bufferedOutput, dictLength, ht);
            maxBlockLength = Math.max(maxBlockLength, uncompressedLength);
        }

        writer.finish();
        meta.writeInt(maxLength);
        // Write one more int for storing max block length.
        meta.writeInt(maxBlockLength);
        meta.writeLong(start);
        meta.writeLong(data.getFilePointer() - start);
        start = data.getFilePointer();
        addressBuffer.copyTo(data);
        meta.writeLong(start);
        meta.writeLong(data.getFilePointer() - start);

        // Now write the reverse terms index
        writeTermsIndex(values);
    }

    private int compressAndGetTermsDictBlockLength(ByteArrayDataOutput bufferedOutput, int dictLength, LZ4.FastCompressionHashTable ht)
        throws IOException {
        int uncompressedLength = bufferedOutput.getPosition() - dictLength;
        data.writeVInt(uncompressedLength);
        LZ4.compressWithDictionary(termsDictBuffer, 0, dictLength, uncompressedLength, data, ht);
        return uncompressedLength;
    }

    private ByteArrayDataOutput maybeGrowBuffer(ByteArrayDataOutput bufferedOutput, int termLength) {
        int pos = bufferedOutput.getPosition(), originalLength = termsDictBuffer.length;
        if (pos + termLength >= originalLength - 1) {
            termsDictBuffer = ArrayUtil.grow(termsDictBuffer, originalLength + termLength);
            bufferedOutput = new ByteArrayDataOutput(termsDictBuffer, pos, termsDictBuffer.length - pos);
        }
        return bufferedOutput;
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

}
