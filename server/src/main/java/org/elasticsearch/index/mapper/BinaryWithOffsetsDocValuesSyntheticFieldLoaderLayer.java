/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.index.fielddata.MultiValuedSortedBinaryDocValues;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

/**
 * Load {@code _source} fields from {@link MultiValuedSortedBinaryDocValues} and associated {@link BinaryDocValues}. The former contains the unique values
 * in sorted order and the latter the offsets for each instance of the values. This allows synthesizing array elements in order as was
 * specified at index time. Note that this works only for leaf arrays.
 */
final class BinaryWithOffsetsDocValuesSyntheticFieldLoaderLayer implements CompositeSyntheticFieldLoader.DocValuesLayer {

    // TODO: this class shares a lot of code with SortedSetWithOffsetsDocValuesSyntheticFieldLoaderLayer, find a way to deduplicate.

    private final String name;
    private final String offsetsFieldName;
    private final Function<BytesRef, BytesRef> converter;
    private DocValuesWithOffsetsLoader docValues;

    /**
     * @param name              The name of the field to synthesize
     * @param offsetsFieldName  The related offset field used to correctly synthesize the field if it is a leaf array
     */
    BinaryWithOffsetsDocValuesSyntheticFieldLoaderLayer(String name, String offsetsFieldName) {
        this(name, offsetsFieldName, Function.identity());
    }

    /**
     * @param name              The name of the field to synthesize
     * @param offsetsFieldName  The related offset field used to correctly synthesize the field if it is a leaf array
     * @param converter         This field value loader layer synthesizes the values read from doc values as utf8 string. If the doc value
     *                          values aren't serializable as utf8 string then it is the responsibility of the converter to covert into a
     *                          format that can be serialized as utf8 string. For example IP field mapper doc values can't directly be
     *                          serialized as utf8 string.
     */
    BinaryWithOffsetsDocValuesSyntheticFieldLoaderLayer(String name, String offsetsFieldName, Function<BytesRef, BytesRef> converter) {
        this.name = Objects.requireNonNull(name);
        this.offsetsFieldName = Objects.requireNonNull(offsetsFieldName);
        this.converter = Objects.requireNonNull(converter);
    }

    @Override
    public String fieldName() {
        return name;
    }

    @Override
    public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
        var valueDocValues = MultiValuedSortedBinaryDocValues.from(leafReader, name);
        SortedDocValues offsetDocValues = DocValues.getSorted(leafReader, offsetsFieldName);

        return docValues = new DocValuesWithOffsetsLoader(valueDocValues, offsetDocValues, converter);
    }

    @Override
    public boolean hasValue() {
        if (docValues != null) {
            return docValues.count() > 0;
        } else {
            return false;
        }
    }

    @Override
    public long valueCount() {
        if (docValues != null) {
            return docValues.count();
        } else {
            return 0;
        }
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        if (docValues != null) {
            docValues.write(b);
        }
    }

    static final class DocValuesWithOffsetsLoader implements DocValuesLoader {
        private final SortedDocValues offsetDocValues;
        private final MultiValuedSortedBinaryDocValues valueDocValues;
        private final Function<BytesRef, BytesRef> converter;
        private final ByteArrayStreamInput scratch = new ByteArrayStreamInput();

        private boolean hasValue;
        private boolean hasOffset;
        private int[] offsetToOrd;

        DocValuesWithOffsetsLoader(
            MultiValuedSortedBinaryDocValues valueDocValues,
            SortedDocValues offsetDocValues,
            Function<BytesRef, BytesRef> converter
        ) {
            this.valueDocValues = valueDocValues;
            this.offsetDocValues = offsetDocValues;
            this.converter = converter;
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

        public int count() {
            if (hasValue) {
                if (offsetToOrd != null) {
                    // HACK: trick CompositeSyntheticFieldLoader to serialize this layer as array.
                    // (if offsetToOrd is not null, then at index time an array was always specified even if there is just one value)
                    return offsetToOrd.length + 1;
                } else {
                    return valueDocValues.docValueCount();
                }
            } else {
                if (hasOffset) {
                    // trick CompositeSyntheticFieldLoader to serialize this layer as empty array.
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
                BytesRef[] values = new BytesRef[valueDocValues.docValueCount()];
                for (int i = 0; i < valueDocValues.docValueCount(); i++) {
                    values[i] = valueDocValues.nextValue().clone();
                }

                for (int offset : offsetToOrd) {
                    if (offset == -1) {
                        b.nullValue();
                        continue;
                    }

                    BytesRef c = values[offset];
                    c = converter.apply(c);
                    b.utf8Value(c.bytes, c.offset, c.length);
                }
            } else if (offsetToOrd != null) {
                // in case all values are NULLs
                for (int offset : offsetToOrd) {
                    assert offset == -1;
                    b.nullValue();
                }
            } else {
                for (int i = 0; i < valueDocValues.docValueCount(); i++) {
                    BytesRef c = valueDocValues.nextValue();
                    c = converter.apply(c);
                    b.utf8Value(c.bytes, c.offset, c.length);
                }
            }
        }
    }

}
