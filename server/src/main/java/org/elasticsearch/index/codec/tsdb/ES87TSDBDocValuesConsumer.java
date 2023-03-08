/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.elasticsearch.core.IOUtils;

import java.io.IOException;
import java.util.Arrays;

final class ES87TSDBDocValuesConsumer extends DocValuesConsumer {

    IndexOutput data, meta;
    final int maxDoc;

    ES87TSDBDocValuesConsumer(SegmentWriteState state, String dataCodec, String dataExtension, String metaCodec, String metaExtension)
        throws IOException {
        boolean success = false;
        try {
            final String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
            data = state.directory.createOutput(dataName, state.context);
            CodecUtil.writeIndexHeader(
                data,
                dataCodec,
                ES87TSDBDocValuesFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            String metaName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
            meta = state.directory.createOutput(metaName, state.context);
            CodecUtil.writeIndexHeader(
                meta,
                metaCodec,
                ES87TSDBDocValuesFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            maxDoc = state.segmentInfo.maxDoc();
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
        meta.writeByte(ES87TSDBDocValuesFormat.NUMERIC);
        writeNumericField(field, new EmptyDocValuesProducer() {
            @Override
            public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
                return DocValues.singleton(valuesProducer.getNumeric(field));
            }
        });
    }

    private long[] writeNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        int numDocsWithValue = 0;
        long numValues = 0;

        SortedNumericDocValues values = valuesProducer.getSortedNumeric(field);
        for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
            numDocsWithValue++;
            final int count = values.docValueCount();
            numValues += count;
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
            values = valuesProducer.getSortedNumeric(field);
            final short jumpTableEntryCount = IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
            meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
            meta.writeShort(jumpTableEntryCount);
            meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
        }
        meta.writeLong(numValues);

        if (numValues > 0) {
            meta.writeInt(ES87TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT);
            final ByteBuffersDataOutput indexOut = new ByteBuffersDataOutput();
            final DirectMonotonicWriter indexWriter = DirectMonotonicWriter.getInstance(
                meta,
                new ByteBuffersIndexOutput(indexOut, "temp-dv-index", "temp-dv-index"),
                1L + ((numValues - 1) >>> ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SHIFT),
                ES87TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT
            );

            final long[] buffer = new long[ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE];
            int bufferSize = 0;
            final long valuesDataOffset = data.getFilePointer();
            final ES87TSDBDocValuesEncoder encoder = new ES87TSDBDocValuesEncoder();

            values = valuesProducer.getSortedNumeric(field);
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                final int count = values.docValueCount();
                for (int i = 0; i < count; ++i) {
                    buffer[bufferSize++] = values.nextValue();
                    if (bufferSize == ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE) {
                        indexWriter.add(data.getFilePointer() - valuesDataOffset);
                        encoder.encode(buffer, data);
                        bufferSize = 0;
                    }
                }
            }
            if (bufferSize > 0) {
                indexWriter.add(data.getFilePointer() - valuesDataOffset);
                // Fill unused slots in the block with zeroes rather than junk
                Arrays.fill(buffer, bufferSize, ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE, 0L);
                encoder.encode(buffer, data);
            }

            final long valuesDataLength = data.getFilePointer() - valuesDataOffset;
            indexWriter.finish();
            final long indexDataOffset = data.getFilePointer();
            data.copyBytes(indexOut.toDataInput(), indexOut.size());
            meta.writeLong(indexDataOffset);
            meta.writeLong(data.getFilePointer() - indexDataOffset);

            meta.writeLong(valuesDataOffset);
            meta.writeLong(valuesDataLength);
        }

        return new long[] { numDocsWithValue, numValues };
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        throw new UnsupportedOperationException("Unsupported binary doc values for field [" + field.name + "]");
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        throw new UnsupportedOperationException("Unsupported sorted doc values for field [" + field.name + "]");
    }

    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        meta.writeInt(field.number);
        meta.writeByte(ES87TSDBDocValuesFormat.SORTED_NUMERIC);
        writeSortedNumericField(field, valuesProducer);
    }

    private void writeSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        long[] stats = writeNumericField(field, valuesProducer);
        int numDocsWithField = Math.toIntExact(stats[0]);
        long numValues = stats[1];
        assert numValues >= numDocsWithField;

        meta.writeInt(numDocsWithField);
        if (numValues > numDocsWithField) {
            long start = data.getFilePointer();
            meta.writeLong(start);
            meta.writeVInt(ES87TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT);

            final DirectMonotonicWriter addressesWriter = DirectMonotonicWriter.getInstance(
                meta,
                data,
                numDocsWithField + 1L,
                ES87TSDBDocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT
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

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        throw new UnsupportedOperationException("Unsupported sorted set doc values for field [" + field.name + "]");
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
