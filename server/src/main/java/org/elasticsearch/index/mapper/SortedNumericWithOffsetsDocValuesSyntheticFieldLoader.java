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
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

class SortedNumericWithOffsetsDocValuesSyntheticFieldLoader extends SourceLoader.DocValuesBasedSyntheticFieldLoader {
    @FunctionalInterface
    interface NumericValueWriter {
        void writeLongValue(XContentBuilder b, long value) throws IOException;
    }

    private final String fullPath;
    private final String leafName;
    private final String offsetsFieldName;
    private final NumericValueWriter valueWriter;
    private NumericDocValuesWithOffsetsLoader docValuesLoader;

    SortedNumericWithOffsetsDocValuesSyntheticFieldLoader(
        String fullPath,
        String leafName,
        String offsetsFieldName,
        NumericValueWriter valueWriter
    ) {
        this.fullPath = fullPath;
        this.leafName = leafName;
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
    public void write(XContentBuilder b) throws IOException {
        if (docValuesLoader != null) {
            docValuesLoader.write(leafName, b);
        }
    }

    @Override
    public String fieldName() {
        return fullPath;
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

        public void write(String fieldName, XContentBuilder b) throws IOException {
            if (hasValue == false && hasOffset == false) {
                return;
            }

            if (offsetToOrd != null && hasValue) {
                int count = valueDocValues.docValueCount();
                long[] values = new long[count];
                for (int i = 0; i < count; i++) {
                    values[i] = valueDocValues.nextValue();
                }

                b.startArray(fieldName);
                for (int offset : offsetToOrd) {
                    if (offset == -1) {
                        b.nullValue();
                    } else {
                        writer.writeLongValue(b, values[offset]);
                    }
                }
                b.endArray();
            } else if (offsetToOrd != null) {
                // in cased all values are NULLs
                b.startArray();
                for (int offset : offsetToOrd) {
                    assert offset == -1;
                    b.nullValue();
                }
                b.endArray();
            } else {
                int count = valueDocValues.docValueCount();
                if (count == 0) {
                    return;
                }
                if (count == 1) {
                    b.field(fieldName);
                } else {
                    b.startArray(fieldName);
                }
                for (int i = 0; i < count; i++) {
                    writer.writeLongValue(b, valueDocValues.nextValue());
                }
                if (count > 1) {
                    b.endArray();
                }
            }
        }

    }
}
