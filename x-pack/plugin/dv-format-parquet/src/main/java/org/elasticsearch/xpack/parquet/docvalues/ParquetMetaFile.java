/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The meta file of a stub segment. It holds the bindings of the fields to Parquet columns and the file pieces that the segment
 * covers, and nothing else, so that opening a segment needs no request to the object store.
 */
final class ParquetMetaFile {

    /**
     * The same extension as the doc values meta file of Lucene. The segment suffix keeps the names apart, and an extension that
     * Elasticsearch already knows needs no entry in {@code LuceneFilesExtensions} in the server.
     */
    static final String EXTENSION = "dvm";
    static final String CODEC_NAME = "ParquetDocValuesMeta";
    static final int VERSION_START = 1;
    static final int VERSION_CURRENT = VERSION_START;

    private ParquetMetaFile() {}

    static void write(SegmentWriteState state, List<ColumnBinding> bindings, List<ExternalFilePiece> pieces) throws IOException {
        String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXTENSION);
        try (IndexOutput out = state.directory.createOutput(fileName, state.context)) {
            CodecUtil.writeIndexHeader(out, CODEC_NAME, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
            out.writeVInt(bindings.size());
            for (ColumnBinding binding : bindings) {
                out.writeString(binding.fieldName());
                out.writeVInt(binding.fieldNumber());
                out.writeString(binding.parquetColumnPath());
                out.writeByte((byte) binding.type().ordinal());
                out.writeByte((byte) binding.conversion().ordinal());
            }
            out.writeVInt(pieces.size());
            for (ExternalFilePiece piece : pieces) {
                out.writeString(piece.location());
                out.writeVLong(piece.length());
                out.writeVLong(piece.indexRowBase());
                out.writeVInt(piece.firstRowGroup());
                out.writeVInt(piece.rowGroupRows().length);
                for (int rows : piece.rowGroupRows()) {
                    out.writeVInt(rows);
                }
                out.writeVInt(piece.columns().size());
                for (ColumnSnapshot column : piece.columns()) {
                    out.writeString(column.path());
                    out.writeVInt(column.physicalType());
                    out.writeZInt(column.convertedType());
                    out.writeByte((byte) (column.required() ? 1 : 0));
                    out.writeVInt(column.codec());
                    for (int rowGroup = 0; rowGroup < column.rowGroups(); rowGroup++) {
                        out.writeVLong(column.offsets()[rowGroup]);
                        out.writeVLong(column.compressedSizes()[rowGroup]);
                        out.writeVLong(column.uncompressedSizes()[rowGroup]);
                        out.writeVLong(column.numValues()[rowGroup]);
                        out.writeZLong(column.minValues()[rowGroup]);
                        out.writeZLong(column.maxValues()[rowGroup]);
                        out.writeZLong(column.nullCounts()[rowGroup]);
                    }
                }
            }
            CodecUtil.writeFooter(out);
        }
    }

    static ParquetDocValuesProducer read(SegmentReadState state) throws IOException {
        String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXTENSION);
        try (ChecksumIndexInput in = state.directory.openChecksumInput(fileName)) {
            Throwable priorException = null;
            List<ColumnBinding> bindings = new ArrayList<>();
            List<ExternalFilePiece> pieces = new ArrayList<>();
            try {
                CodecUtil.checkIndexHeader(in, CODEC_NAME, VERSION_START, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
                int bindingCount = in.readVInt();
                for (int i = 0; i < bindingCount; i++) {
                    String fieldName = in.readString();
                    int fieldNumber = in.readVInt();
                    String column = in.readString();
                    DocValuesType type = DocValuesType.values()[in.readByte()];
                    Conversion conversion = Conversion.values()[in.readByte()];
                    bindings.add(new ColumnBinding(fieldName, fieldNumber, column, type, conversion));
                }
                int pieceCount = in.readVInt();
                for (int i = 0; i < pieceCount; i++) {
                    String location = in.readString();
                    long length = in.readVLong();
                    long indexRowBase = in.readVLong();
                    int firstRowGroup = in.readVInt();
                    int[] rowGroupRows = new int[in.readVInt()];
                    for (int g = 0; g < rowGroupRows.length; g++) {
                        rowGroupRows[g] = in.readVInt();
                    }
                    int columnCount = in.readVInt();
                    List<ColumnSnapshot> columns = new ArrayList<>(columnCount);
                    for (int c = 0; c < columnCount; c++) {
                        String path = in.readString();
                        int physicalType = in.readVInt();
                        int convertedType = in.readZInt();
                        boolean required = in.readByte() == 1;
                        int codec = in.readVInt();
                        long[] offsets = new long[rowGroupRows.length];
                        long[] compressed = new long[rowGroupRows.length];
                        long[] uncompressed = new long[rowGroupRows.length];
                        long[] values = new long[rowGroupRows.length];
                        long[] minimums = new long[rowGroupRows.length];
                        long[] maximums = new long[rowGroupRows.length];
                        long[] nullCounts = new long[rowGroupRows.length];
                        for (int g = 0; g < rowGroupRows.length; g++) {
                            offsets[g] = in.readVLong();
                            compressed[g] = in.readVLong();
                            uncompressed[g] = in.readVLong();
                            values[g] = in.readVLong();
                            minimums[g] = in.readZLong();
                            maximums[g] = in.readZLong();
                            nullCounts[g] = in.readZLong();
                        }
                        columns.add(
                            new ColumnSnapshot(
                                path,
                                physicalType,
                                convertedType,
                                required,
                                codec,
                                offsets,
                                compressed,
                                uncompressed,
                                values,
                                minimums,
                                maximums,
                                nullCounts
                            )
                        );
                    }
                    pieces.add(new ExternalFilePiece(location, length, indexRowBase, firstRowGroup, rowGroupRows, columns));
                }
            } catch (Throwable t) {
                priorException = t;
            } finally {
                CodecUtil.checkFooter(in, priorException);
            }
            return new ParquetDocValuesProducer(bindings, pieces, ParquetReadContext.current());
        }
    }
}
