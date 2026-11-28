/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.escf.ColumnarOffsetsBuilder;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.escf.EscfColumnData;
import org.elasticsearch.escf.EscfColumnKind;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class SortedNumericWithOffsetsDocValuesSyntheticFieldLoaderLayer implements CompositeSyntheticFieldLoader.DocValuesLayer {
    @FunctionalInterface
    public interface NumericValueWriter {
        void writeLongValue(XContentBuilder b, long value) throws IOException;
    }

    private final String fullPath;
    private final String offsetsFieldName;
    private final NumericValueWriter valueWriter;
    private NumericDocValuesWithOffsetsLoader docValuesLoader;

    public SortedNumericWithOffsetsDocValuesSyntheticFieldLoaderLayer(
        String fullPath,
        String offsetsFieldName,
        NumericValueWriter valueWriter
    ) {
        this.fullPath = fullPath;
        this.offsetsFieldName = offsetsFieldName;
        this.valueWriter = valueWriter;
    }

    @Override
    public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
        SortedNumericDocValues valueDocValues = DocValues.getSortedNumeric(leafReader, fullPath);
        SortedDocValues offsetDocValues = DocValues.getSorted(leafReader, offsetsFieldName);

        return docValuesLoader = new NumericDocValuesWithOffsetsLoader(valueDocValues, offsetDocValues, valueWriter);
    }

    @Override
    public boolean hasValue() {
        return docValuesLoader != null && docValuesLoader.hasValue();
    }

    @Override
    public long valueCount() {
        if (docValuesLoader != null) {
            return docValuesLoader.count();
        } else {
            return 0;
        }
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        if (docValuesLoader != null) {
            docValuesLoader.write(b);
        }
    }

    @Override
    public String fieldName() {
        return fullPath;
    }

    /**
     * The direct counterpart of the numeric loader layers on the columnar batch path. It reads {@code values}, the sortable-long
     * column the number mapper has just built, which still holds every document's values in source order with duplicates, so there is
     * no offsets sidecar to undo a sort with. What it has to reproduce is the shape rule: the row path records offsets only for arrays
     * of at least {@link ColumnarOffsetsBuilder#MIN_RECORDED_SLOTS} slots ({@code FieldArrayContext#addToLuceneDocument}), and a
     * recorded array renders as an array even when its values collapse to one ({@link NumericDocValuesWithOffsetsLoader#count}), so
     * a document with that many slots reports one value more than it has. A single value, or a single-element array, is a scalar;
     * an empty array is absent. Without offsets ({@link SortedNumericDocValuesSyntheticFieldLoaderLayer}) the batch path only lets a
     * document through with at most one value, so the same reading holds.
     *
     * @param values          a {@code LONG} column, or an {@code ARRAY} column of {@code LONG} elements, in sortable-long encoding
     * @param offsetsRecorded whether the mapper records the offsets sidecar for this field, which is what makes a short array render
     *                        as an array
     * @param valueWriter     formats a sortable long as the field's value, the same writer the loader layer uses
     */
    public static SourceColumn.Layer sourceLayer(EscfColumnData values, boolean offsetsRecorded, NumericValueWriter valueWriter) {
        return new SourceLayer(values, offsetsRecorded, valueWriter);
    }

    private static final class SourceLayer implements SourceColumn.Layer {
        private final EscfColumn column;
        /** Element column of an {@code ARRAY}, or {@code column} itself for a scalar {@code LONG} column. */
        private final EscfColumn elements;
        /** Per-document element ranges of an {@code ARRAY} column; {@code null} for a scalar column. */
        @Nullable
        private final int[] rowOffsets;
        private final boolean offsetsRecorded;
        private final NumericValueWriter valueWriter;

        private int from;
        private int to;

        SourceLayer(EscfColumnData values, boolean offsetsRecorded, NumericValueWriter valueWriter) {
            this.column = EscfColumn.from(values);
            if (values.kind() == EscfColumnKind.ARRAY) {
                assert values.child().kind() == EscfColumnKind.LONG : EscfColumnKind.name(values.child().kind());
                this.elements = EscfColumn.from(values.child());
                this.rowOffsets = values.offsets();
            } else {
                assert values.kind() == EscfColumnKind.LONG : EscfColumnKind.name(values.kind());
                this.elements = column;
                this.rowOffsets = null;
            }
            this.offsetsRecorded = offsetsRecorded;
            this.valueWriter = valueWriter;
        }

        @Override
        public void advance(int doc) {
            if (rowOffsets == null) {
                from = doc;
                to = column.isPresent(doc) ? doc + 1 : doc;
            } else {
                // Absent documents and empty arrays are both zero-width ranges, and both render as nothing.
                from = rowOffsets[doc];
                to = rowOffsets[doc + 1];
            }
        }

        @Override
        public long valueCount() {
            final int slots = to - from;
            return offsetsRecorded && slots >= ColumnarOffsetsBuilder.MIN_RECORDED_SLOTS ? slots + 1 : slots;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            for (int i = from; i < to; i++) {
                valueWriter.writeLongValue(b, elements.getLongValue(i));
            }
        }
    }

    private static final class NumericDocValuesWithOffsetsLoader implements DocValuesLoader {
        private final SortedDocValues offsetDocValues;
        private final SortedNumericDocValues valueDocValues;
        private final NumericValueWriter writer;
        private final ByteArrayStreamInput scratch = new ByteArrayStreamInput();

        private boolean hasValue;
        private boolean hasOffset;
        private int[] offsetToOrd;

        NumericDocValuesWithOffsetsLoader(
            SortedNumericDocValues valueDocValues,
            SortedDocValues offsetDocValues,
            NumericValueWriter writer
        ) {
            this.valueDocValues = valueDocValues;
            this.offsetDocValues = offsetDocValues;
            this.writer = writer;
        }

        @Override
        public boolean advanceToDoc(int docId) throws IOException {
            hasValue = valueDocValues.advanceExact(docId);
            hasOffset = offsetDocValues.advanceExact(docId);
            if (hasValue || hasOffset) {
                if (hasOffset) {
                    int offsetOrd = offsetDocValues.ordValue();
                    var encodedValue = offsetDocValues.lookupOrd(offsetOrd);
                    scratch.reset(encodedValue.bytes, encodedValue.offset, encodedValue.length);
                    offsetToOrd = FieldArrayContext.parseOffsetArray(scratch);
                } else {
                    offsetToOrd = null;
                }
                return true;
            } else {
                offsetToOrd = null;
                return false;
            }
        }

        public boolean hasValue() {
            return hasOffset || (hasValue && valueDocValues.docValueCount() > 0);
        }

        public int count() {
            if (hasValue) {
                if (offsetToOrd != null && offsetToOrd.length > 0) {
                    // Even though there may only be one value, the fact that offsets were recorded means that
                    // the value was in an array, so we need to trick CompositeSyntheticFieldLoader into
                    // always serializing this layer as an array
                    return offsetToOrd.length + 1;
                } else {
                    // Either no offsets or empty offsets (offsetToOrd.length == 0): fall back to writing
                    // the raw doc values. An empty offset array can arise when markEmptyArray() was called
                    // for one occurrence of the field (e.g. field: [[]]) while a separate occurrence in the
                    // same document contributed a numeric doc value without recording an offset. In that
                    // case offsetToOrd carries no positional info, so we emit the values without ordering.
                    return valueDocValues.docValueCount();
                }
            } else {
                if (hasOffset) {
                    // same as above, even though there are no values, the presence of recorded offsets means
                    // there was an array containing zero or more null values in the original source
                    return 2;
                } else {
                    return 0;
                }
            }
        }

        public void write(XContentBuilder b) throws IOException {
            if (hasValue == false && hasOffset == false) {
                return;
            }

            if (offsetToOrd != null && hasValue && offsetToOrd.length > 0) {
                int count = valueDocValues.docValueCount();
                long[] values = new long[count];
                int duplicates = 0;
                for (int i = 0; i < count; i++) {
                    long value = valueDocValues.nextValue();
                    if (i > 0 && value == values[i - duplicates - 1]) {
                        duplicates++;
                        continue;
                    }

                    values[i - duplicates] = value;
                }

                for (int offset : offsetToOrd) {
                    if (offset == -1) {
                        b.nullValue();
                    } else {
                        writer.writeLongValue(b, values[offset]);
                    }
                }
            } else if (offsetToOrd != null && hasValue == false) {
                // all values are NULLs (or empty array: offsetToOrd is empty)
                for (int offset : offsetToOrd) {
                    assert offset == -1;
                    b.nullValue();
                }
            } else {
                // no offsets, or empty offsets with values: write doc values directly
                for (int i = 0; i < valueDocValues.docValueCount(); i++) {
                    writer.writeLongValue(b, valueDocValues.nextValue());
                }
            }
        }

    }
}
