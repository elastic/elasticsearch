/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.columnar.numeric.ColumnarNumericBinaryDocValues;
import org.elasticsearch.columnar.numeric.NumericColumnMetadata;
import org.elasticsearch.columnar.numeric.NumericColumnReader;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnarCodecUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Reads the columns written by {@link ColumNARDocValuesConsumer}, served at the {@code BINARY} surface
 * ({@link #getBinary}). Fields are framed generically ({@code fieldNumber}, {@link ColumnarFieldType} id,
 * metadata), so a new column type slots in by extending the read dispatch here.
 */
final class ColumNARDocValuesProducer extends DocValuesProducer {

    private final int maxDoc;
    private final IndexInput data;
    private final Map<Integer, Column> columns = new HashMap<>();
    private boolean closed = false;

    /** A read-side column: its declared type and the metadata needed to open it. */
    private record Column(ColumnarFieldType type, NumericColumnMetadata numeric) {}

    ColumNARDocValuesProducer(SegmentReadState state) throws IOException {
        this.maxDoc = state.segmentInfo.maxDoc();

        String metaName = IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            ColumNARDocValuesFormat.META_EXTENSION
        );
        FormatVersion metaVersion = null;
        try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaName)) {
            Throwable priorException = null;
            try {
                metaVersion = ColumnarCodecUtil.checkHeader(
                    meta,
                    ColumNARDocValuesFormat.META_CODEC,
                    state.segmentInfo.getId(),
                    state.segmentSuffix
                );
                for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
                    columns.put(fieldNumber, readColumn(ColumnarFieldType.fromId(meta.readByte()), meta, metaVersion));
                }
            } catch (Throwable e) {
                priorException = e;
            } finally {
                CodecUtil.checkFooter(meta, priorException);
            }
        }

        boolean success = false;
        try {
            String dataName = IndexFileNames.segmentFileName(
                state.segmentInfo.name,
                state.segmentSuffix,
                ColumNARDocValuesFormat.DATA_EXTENSION
            );
            data = state.directory.openInput(dataName, state.context);
            final FormatVersion dataVersion = ColumnarCodecUtil.checkHeader(
                data,
                ColumNARDocValuesFormat.DATA_CODEC,
                state.segmentInfo.getId(),
                state.segmentSuffix
            );
            if (metaVersion.equals(dataVersion) == false) {
                throw new CorruptIndexException(
                    "Format versions mismatch: meta=" + metaVersion.version() + ", data=" + dataVersion.version(),
                    data
                );
            }
            CodecUtil.retrieveChecksum(data);
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this);
            }
        }
    }

    private Column readColumn(ColumnarFieldType type, ChecksumIndexInput meta, final FormatVersion formatVersion) throws IOException {
        return switch (type) {
            case LONG, DOUBLE -> new Column(type, NumericColumnMetadata.readFrom(meta, maxDoc, formatVersion));
            case STRING -> throw new UnsupportedOperationException("ColumNAR [" + type + "] column is not implemented yet");
        };
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        Column column = columns.get(field.number);
        if (column == null) {
            throw new IllegalStateException("field [" + field.name + "] is not a ColumNAR column");
        }
        return switch (column.type()) {
            case LONG, DOUBLE -> numericBinary(column.numeric());
            case STRING -> throw new UnsupportedOperationException("ColumNAR [" + column.type() + "] column is not implemented yet");
        };
    }

    private BinaryDocValues numericBinary(NumericColumnMetadata metadata) throws IOException {
        NumericColumnReader reader = new NumericColumnReader(metadata, data);
        ColumnIterator iterator = reader.iterator();
        return new ColumnarNumericBinaryDocValues(reader, iterator, maxDoc, metadata.skipper(), data);
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) {
        throw typedNotSupported("numeric");
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
        throw typedNotSupported("sorted-numeric");
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) {
        throw typedNotSupported("sorted");
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) {
        throw typedNotSupported("sorted-set");
    }

    @Override
    public DocValuesSkipper getSkipper(FieldInfo field) {
        // Deliberately unsupported: a ColumNAR column is exposed as a BINARY field, and a BINARY field
        // cannot carry a Lucene skip index, so Lucene never calls this. Range pushdown does not rely on
        // a Lucene skipper — it uses the column's own skip index, driven by ColumnarNumericRangeQuery
        // through the binary reader.
        throw typedNotSupported("skipper");
    }

    private static UnsupportedOperationException typedNotSupported(String shape) {
        return new UnsupportedOperationException(
            "ColumNAR is a binary doc-values format and does not serve " + shape + " doc values; read the column at getBinary"
        );
    }

    @Override
    public void checkIntegrity() throws IOException {
        CodecUtil.checksumEntireFile(data);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        IOUtils.close(data);
    }
}
