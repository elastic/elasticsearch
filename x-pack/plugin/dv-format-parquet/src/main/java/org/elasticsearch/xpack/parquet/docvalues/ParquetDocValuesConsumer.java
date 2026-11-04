/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Writes the reference to external Parquet files and no values. The only way in is {@link #merge}, which {@code IndexWriter#addIndexes}
 * reaches with a {@link ExternalParquetCodecReader}: the consumer then copies the reference of the source and does not read a value.
 * Flush of indexed documents is not supported, because the values live in the Parquet file and not in this index.
 */
final class ParquetDocValuesConsumer extends DocValuesConsumer {

    private final SegmentWriteState state;
    private final List<ColumnBinding> bindings = new ArrayList<>();
    private final List<ExternalFilePiece> pieces = new ArrayList<>();
    private boolean merged;

    ParquetDocValuesConsumer(SegmentWriteState state) {
        this.state = state;
    }

    @Override
    public void merge(MergeState mergeState) throws IOException {
        if (merged) {
            throw new IllegalStateException("a stub segment references its files once");
        }
        if (mergeState.needsIndexSort) {
            throw new UnsupportedOperationException("a segment that references Parquet files cannot be index sorted");
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
        Map<String, ColumnBinding> sourceBindings = new java.util.HashMap<>();
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
            bindings.add(
                new ColumnBinding(binding.fieldName(), fieldInfo.number, binding.parquetColumnPath(), binding.type(), binding.conversion())
            );
        }
        pieces.addAll(source.pieces());
        merged = true;
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
            "field [" + field.name + "] cannot be indexed, because its doc values are served from external Parquet files"
        );
    }

    @Override
    public void close() throws IOException {
        if (merged) {
            ParquetMetaFile.write(state, bindings, pieces);
        }
    }
}
