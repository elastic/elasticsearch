/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.dv;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Doc values format whose values live in external Parquet files and are read through the ES|QL Parquet reader. A segment written
 * with this format is a stub that only references the files, so that the format name stored in the segment resolves to this class
 * when the segment is opened.
 */
public final class ParquetDocValuesFormat extends DocValuesFormat {

    /** Distinct from any other Parquet-backed format, because the name is stored in segments and resolved through SPI. */
    public static final String NAME = "ESParquetEsql900";

    public ParquetDocValuesFormat() {
        super(NAME);
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new ReferenceConsumer(state);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return MetaFile.read(state);
    }

    /**
     * The only file of a stub segment that this format writes: the file piece and the column bindings. There are no values in it.
     */
    static final class MetaFile {

        /**
         * A known Lucene extension, because the store checks the extensions of segment files; the segment suffix keeps it apart from
         * Lucene's own.
         */
        static final String EXTENSION = "dvm";
        private static final String CODEC = "ESParquetEsqlMeta";
        private static final int VERSION = 2;
        private static final byte NUMERIC = 0;
        private static final byte BINARY = 1;

        private MetaFile() {}

        static void write(SegmentWriteState state, List<ColumnBinding> bindings, ExternalFilePiece piece) throws IOException {
            String name = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXTENSION);
            try (IndexOutput out = state.directory.createOutput(name, state.context)) {
                CodecUtil.writeIndexHeader(out, CODEC, VERSION, state.segmentInfo.getId(), state.segmentSuffix);
                out.writeString(piece.location());
                out.writeVLong(piece.length());
                out.writeVLong(piece.firstRow());
                out.writeVInt(piece.rowCount());
                out.writeVLong(piece.indexRowBase());
                out.writeVInt(piece.rowGroupRows().length);
                for (int rows : piece.rowGroupRows()) {
                    out.writeVInt(rows);
                }
                out.writeVInt(piece.stats().size());
                for (Map.Entry<String, ExternalFilePiece.ColumnStats> entry : piece.stats().entrySet()) {
                    out.writeString(entry.getKey());
                    ExternalFilePiece.ColumnStats stats = entry.getValue();
                    for (int i = 0; i < piece.rowGroupRows().length; i++) {
                        out.writeLong(stats.minValues()[i]);
                        out.writeLong(stats.maxValues()[i]);
                        out.writeVInt(stats.docCounts()[i]);
                    }
                }
                out.writeVInt(bindings.size());
                for (ColumnBinding binding : bindings) {
                    out.writeString(binding.fieldName());
                    out.writeVInt(binding.fieldNumber());
                    out.writeString(binding.parquetColumn());
                    out.writeByte(binding.type() == DocValuesType.NUMERIC ? NUMERIC : BINARY);
                    out.writeString(binding.conversion().externalName());
                }
                CodecUtil.writeFooter(out);
            }
        }

        static ParquetDocValuesProducer read(SegmentReadState state) throws IOException {
            String name = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXTENSION);
            try (ChecksumIndexInput in = state.directory.openChecksumInput(name)) {
                CodecUtil.checkIndexHeader(in, CODEC, VERSION, VERSION, state.segmentInfo.getId(), state.segmentSuffix);
                String location = in.readString();
                long length = in.readVLong();
                long firstRow = in.readVLong();
                int rowCount = in.readVInt();
                long indexRowBase = in.readVLong();
                int[] rowGroupRows = new int[in.readVInt()];
                for (int i = 0; i < rowGroupRows.length; i++) {
                    rowGroupRows[i] = in.readVInt();
                }
                int statsCount = in.readVInt();
                Map<String, ExternalFilePiece.ColumnStats> stats = new HashMap<>();
                for (int s = 0; s < statsCount; s++) {
                    String column = in.readString();
                    long[] minValues = new long[rowGroupRows.length];
                    long[] maxValues = new long[rowGroupRows.length];
                    int[] docCounts = new int[rowGroupRows.length];
                    for (int i = 0; i < rowGroupRows.length; i++) {
                        minValues[i] = in.readLong();
                        maxValues[i] = in.readLong();
                        docCounts[i] = in.readVInt();
                    }
                    stats.put(column, new ExternalFilePiece.ColumnStats(minValues, maxValues, docCounts));
                }
                ExternalFilePiece piece = new ExternalFilePiece(location, length, firstRow, rowCount, indexRowBase, rowGroupRows, stats);
                int count = in.readVInt();
                List<ColumnBinding> bindings = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    String fieldName = in.readString();
                    int fieldNumber = in.readVInt();
                    String column = in.readString();
                    byte type = in.readByte();
                    String conversion = in.readString();
                    bindings.add(
                        new ColumnBinding(
                            fieldName,
                            fieldNumber,
                            column,
                            type == NUMERIC ? DocValuesType.NUMERIC : DocValuesType.BINARY,
                            Conversion.fromExternalName(conversion)
                        )
                    );
                }
                CodecUtil.checkFooter(in);
                ParquetDocValuesProducer producer = new ParquetDocValuesProducer(bindings, piece, ParquetReadContext.current());
                producer.resolveStorage();
                return producer;
            }
        }
    }

    /**
     * Writes the reference to a Parquet file and no values. The only way in is {@link #merge}, which {@code IndexWriter#addIndexes}
     * reaches with an {@code ExternalParquetCodecReader}: the consumer copies the reference of that source and reads no value. Flushing
     * indexed documents is not supported, because the values live in the Parquet file and not in this index.
     */
    static final class ReferenceConsumer extends DocValuesConsumer {

        private final SegmentWriteState state;
        private final List<ColumnBinding> bindings = new ArrayList<>();
        private ExternalFilePiece piece;

        ReferenceConsumer(SegmentWriteState state) {
            this.state = state;
        }

        @Override
        public void merge(MergeState mergeState) throws IOException {
            if (piece != null) {
                throw new IllegalStateException("a stub segment references its file once");
            }
            if (mergeState.needsIndexSort) {
                throw new UnsupportedOperationException("a segment that references a Parquet file cannot be index sorted");
            }
            ParquetDocValuesProducer source = null;
            for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
                DocValuesProducer producer = mergeState.docValuesProducers[i];
                if (producer == null) {
                    continue;
                }
                if (source != null || (producer instanceof ParquetDocValuesProducer) == false) {
                    throw new UnsupportedOperationException("merging segments that reference Parquet files is not supported");
                }
                source = (ParquetDocValuesProducer) producer;
                // The only supported source is a stub that is attached whole, so every document keeps its docID.
                int maxDoc = mergeState.maxDocs[i];
                if (maxDoc > 0 && (mergeState.docMaps[i].get(0) != 0 || mergeState.docMaps[i].get(maxDoc - 1) != maxDoc - 1)) {
                    throw new UnsupportedOperationException("a stub segment cannot be attached with deleted or reordered documents");
                }
            }
            if (source == null) {
                throw new IllegalStateException("no Parquet source to attach");
            }
            Map<String, ColumnBinding> sourceBindings = new HashMap<>();
            for (ColumnBinding binding : source.bindings()) {
                sourceBindings.put(binding.fieldName(), binding);
            }
            for (FieldInfo fieldInfo : mergeState.mergeFieldInfos) {
                if (fieldInfo.getDocValuesType() == DocValuesType.NONE) {
                    continue;
                }
                ColumnBinding binding = sourceBindings.get(fieldInfo.name);
                if (binding == null) {
                    throw new IllegalStateException("field [" + fieldInfo.name + "] has doc values but is not bound to a Parquet column");
                }
                // the field number of the new segment can differ from the one of the source
                bindings.add(
                    new ColumnBinding(binding.fieldName(), fieldInfo.number, binding.parquetColumn(), binding.type(), binding.conversion())
                );
            }
            piece = source.piece();
        }

        @Override
        public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) {
            throw unsupportedFlush(field);
        }

        @Override
        public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) {
            throw unsupportedFlush(field);
        }

        @Override
        public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) {
            throw unsupportedFlush(field);
        }

        @Override
        public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) {
            throw unsupportedFlush(field);
        }

        @Override
        public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) {
            throw unsupportedFlush(field);
        }

        private static UnsupportedOperationException unsupportedFlush(FieldInfo field) {
            return new UnsupportedOperationException(
                "field [" + field.name + "] cannot be indexed, because its doc values are served from a Parquet file"
            );
        }

        @Override
        public void close() throws IOException {
            if (piece != null) {
                MetaFile.write(state, bindings, piece);
            }
        }
    }
}
