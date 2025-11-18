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
                if (offsetToOrd != null) {
                    // Even though there may only be one value, the fact that offsets were recorded means that
                    // the value was in an array, so we need to trick CompositeSyntheticFieldLoader into
                    // always serializing this layer as an array
                    return offsetToOrd.length + 1;
                } else {
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

            if (offsetToOrd != null && hasValue) {
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
            } else if (offsetToOrd != null) {
                // in cased all values are NULLs
                for (int offset : offsetToOrd) {
                    assert offset == -1;
                    b.nullValue();
                }
            } else {
                for (int i = 0; i < valueDocValues.docValueCount(); i++) {
                    writer.writeLongValue(b, valueDocValues.nextValue());
                }
            }
        }

    }
}
