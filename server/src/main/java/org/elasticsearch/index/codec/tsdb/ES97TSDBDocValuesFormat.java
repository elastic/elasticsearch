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
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.elasticsearch.core.IOUtils;

import java.io.IOException;

public final class ES97TSDBDocValuesFormat extends DocValuesFormat {

    private static final String CODEC_NAME = "ES97TSDB";

    private static final String TSID_CODEC_NAME = "ES97TSDBTSID";

    private static final String TSID_EXTENSION = "tsid";

    private static final int VERSION_START = 1;

    private static final int VERSION_CURRENT = VERSION_START;

    private static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

    private final DocValuesFormat format;

    public ES97TSDBDocValuesFormat(DocValuesFormat format) {
        super(CODEC_NAME);
        this.format = format;
    }

    public ES97TSDBDocValuesFormat() {
        super(CODEC_NAME);
        // This constructor seems to be called during merges
        format = new Lucene90DocValuesFormat();
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new ES97TSDBDocValuesConsumer(state, format.fieldsConsumer(state));
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new ES97TSDBDocValuesProducer(state, format.fieldsProducer(state));
    }

    private static class ES97TSDBDocValuesProducer extends DocValuesProducer {

        private final IndexInput tsidData;

        private final DocValuesProducer producer;

        private final long pos;

        ES97TSDBDocValuesProducer(SegmentReadState state, DocValuesProducer producer) throws IOException {
            this.producer = producer;
            String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, TSID_EXTENSION);
            this.tsidData = state.directory.openInput(dataName, state.context);
            boolean success = false;
            try {
                CodecUtil.checkIndexHeader(
                    tsidData,
                    TSID_CODEC_NAME,
                    VERSION_START,
                    VERSION_CURRENT,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                this.pos = tsidData.getFilePointer();
                // NOTE: data file is too costly to verify checksum against all the bytes on open,
                // but for now we at least verify proper structure of the checksum footer: which looks
                // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
                // such as file truncation.
                CodecUtil.retrieveChecksum(tsidData);
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(this.tsidData);
                }
            }
        }

        @Override
        public NumericDocValues getNumeric(FieldInfo field) {
            throw new UnsupportedOperationException("Unsupported numeric doc values for field [" + field.name + "]");
        }

        @Override
        public BinaryDocValues getBinary(FieldInfo field) {
            throw new UnsupportedOperationException("Unsupported binary doc values for field [" + field.name + "]");
        }

        @Override
        public SortedDocValues getSorted(FieldInfo field) throws IOException {
            final SortedDocValues sorted = producer.getSorted(field);
            tsidData.seek(pos);
            final int blockShift = tsidData.readInt();
            final DirectMonotonicReader.Meta values = DirectMonotonicReader.loadMeta(tsidData, sorted.getValueCount(), blockShift);
            final long start = tsidData.readLong();
            final long length = tsidData.readLong();
            final RandomAccessInput addressesSlice = tsidData.randomAccessSlice(start, length);
            final DirectMonotonicReader ordsReader = DirectMonotonicReader.getInstance(values, addressesSlice);
            return new TSIDSortedDocValues(sorted) {
                @Override
                public int advanceOrd() throws IOException {
                    int doc = docID();
                    if (doc == DocIdSetIterator.NO_MORE_DOCS || doc == -1) {
                        return doc;
                    }
                    final int nextDoc = (int) ordsReader.get(sorted.ordValue());
                    sorted.advanceExact(nextDoc);
                    return nextDoc;
                }
            };
        }

        @Override
        public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
            throw new UnsupportedOperationException("Unsupported sorted numeric doc values for field [" + field.name + "]");
        }

        @Override
        public SortedSetDocValues getSortedSet(FieldInfo field) {
            throw new UnsupportedOperationException("Unsupported sorted set doc values for field [" + field.name + "]");
        }

        @Override
        public void checkIntegrity() throws IOException {
            producer.checkIntegrity();
            CodecUtil.checksumEntireFile(tsidData);
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(producer, tsidData);
        }
    }

    private static class ES97TSDBDocValuesConsumer extends DocValuesConsumer {
        private IndexOutput tsidData;

        private final DocValuesConsumer consumer;

        ES97TSDBDocValuesConsumer(SegmentWriteState state, DocValuesConsumer consumer) throws IOException {
            this.consumer = consumer;
            boolean success = false;
            try {
                String dataName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, TSID_EXTENSION);
                tsidData = state.directory.createOutput(dataName, state.context);
                CodecUtil.writeIndexHeader(tsidData, TSID_CODEC_NAME, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(this);
                }
            }
        }

        @Override
        public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) {
            throw new UnsupportedOperationException("Unsupported numeric doc values for field [" + field.name + "]");
        }

        @Override
        public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            throw new UnsupportedOperationException("Unsupported binary doc values for field [" + field.name + "]");
        }

        @Override
        public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            consumer.addSortedField(field, valuesProducer);
            final TSIDSortedDocValues values = new TSIDSortedDocValues(valuesProducer.getSorted(field));
            final ByteBuffersDataOutput addressBuffer = new ByteBuffersDataOutput();
            tsidData.writeInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
            final DirectMonotonicWriter writer = DirectMonotonicWriter.getInstance(
                tsidData,
                new ByteBuffersIndexOutput(addressBuffer, "temp", "temp"),
                values.getValueCount(),
                DIRECT_MONOTONIC_BLOCK_SHIFT
            );
            values.nextDoc();
            do {
                values.advanceOrd();
                writer.add(values.docID());
            } while (values.docID() != DocIdSetIterator.NO_MORE_DOCS);
            writer.finish();

            final long start = tsidData.getFilePointer();
            tsidData.writeLong(start + 16);
            tsidData.writeLong(addressBuffer.size());
            addressBuffer.copyTo(tsidData);
        }

        @Override
        public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) {
            throw new UnsupportedOperationException("Unsupported sorted numeric doc values for field [" + field.name + "]");
        }

        @Override
        public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) {
            throw new UnsupportedOperationException("Unsupported sorted set doc values for field [" + field.name + "]");
        }

        @Override
        public void close() throws IOException {
            boolean success = false;
            try {
                consumer.close();
                if (tsidData != null) {
                    CodecUtil.writeFooter(tsidData); // write checksum
                }
                success = true;
            } finally {
                if (success) {
                    IOUtils.close(tsidData);
                } else {
                    IOUtils.closeWhileHandlingException(tsidData);
                }
                tsidData = null;
            }
        }
    }
}
