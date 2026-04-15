/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.index.fielddata.MultiValuedSortedBinaryDocValues;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

/**
 *  Doc values loader for UTF-8 multivalued fields with offset reconstruction, used by
 * {@link SortedSetWithOffsetsDocValuesSyntheticFieldLoaderLayer} and {@link BinaryWithOffsetsDocValuesSyntheticFieldLoaderLayer}.
 */
final class ValuesWithOffsetsDocValuesLoader implements SourceLoader.SyntheticFieldLoader.DocValuesLoader {

    static ValuesWithOffsetsDocValuesLoader sortedSetLoader(
        SortedSetDocValues valueDocValues,
        SortedDocValues offsetDocValues,
        Function<BytesRef, BytesRef> converter
    ) {
        return new ValuesWithOffsetsDocValuesLoader(offsetDocValues, new SortedSetValuesSource(valueDocValues), converter);
    }

    static ValuesWithOffsetsDocValuesLoader binaryLoader(
        MultiValuedSortedBinaryDocValues valueDocValues,
        SortedDocValues offsetDocValues,
        Function<BytesRef, BytesRef> converter
    ) {
        return new ValuesWithOffsetsDocValuesLoader(offsetDocValues, new MultivaluedBinarySortedValuesSource(valueDocValues), converter);
    }

    /**
     * Multivalued sorted per-document values. Used with sorted offset doc values to reconstruct leaf array order for
     * synthetic {@code _source}.
     * <p>
     * {@link #writeUtf8ValuesWithOffsets} is only called when offsets are available
     */
    private interface SortedValuesSource {
        boolean advanceExact(int doc) throws IOException;

        int docValueCount();

        void writeSequentialUtf8Values(XContentBuilder b, Function<BytesRef, BytesRef> converter) throws IOException;

        void writeUtf8ValuesWithOffsets(XContentBuilder b, int[] offsetToOrd, Function<BytesRef, BytesRef> converter) throws IOException;
    }

    private record SortedSetValuesSource(SortedSetDocValues valueDocValues) implements SortedValuesSource {
        private SortedSetValuesSource(SortedSetDocValues valueDocValues) {
            this.valueDocValues = Objects.requireNonNull(valueDocValues);
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            return valueDocValues.advanceExact(doc);
        }

        @Override
        public int docValueCount() {
            return valueDocValues.docValueCount();
        }

        @Override
        public void writeSequentialUtf8Values(XContentBuilder b, Function<BytesRef, BytesRef> converter) throws IOException {
            for (int i = 0; i < valueDocValues.docValueCount(); i++) {
                BytesRef c = valueDocValues.lookupOrd(valueDocValues.nextOrd());
                c = converter.apply(c);
                b.utf8Value(c.bytes, c.offset, c.length);
            }
        }

        @Override
        public void writeUtf8ValuesWithOffsets(XContentBuilder b, int[] offsetToOrd, Function<BytesRef, BytesRef> converter)
            throws IOException {
            long[] ords = new long[valueDocValues.docValueCount()];
            for (int i = 0; i < valueDocValues.docValueCount(); i++) {
                ords[i] = valueDocValues.nextOrd();
            }

            for (int offset : offsetToOrd) {
                if (offset == -1) {
                    b.nullValue();
                    continue;
                }

                long ord = ords[offset];
                BytesRef c = valueDocValues.lookupOrd(ord);
                c = converter.apply(c);
                b.utf8Value(c.bytes, c.offset, c.length);
            }
        }
    }

    private record MultivaluedBinarySortedValuesSource(MultiValuedSortedBinaryDocValues valueDocValues) implements SortedValuesSource {
        private MultivaluedBinarySortedValuesSource(MultiValuedSortedBinaryDocValues valueDocValues) {
            this.valueDocValues = Objects.requireNonNull(valueDocValues);
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            return valueDocValues.advanceExact(doc);
        }

        @Override
        public int docValueCount() {
            return valueDocValues.docValueCount();
        }

        @Override
        public void writeSequentialUtf8Values(XContentBuilder b, Function<BytesRef, BytesRef> converter) throws IOException {
            for (int i = 0; i < valueDocValues.docValueCount(); i++) {
                BytesRef c = valueDocValues.nextValue();
                c = converter.apply(c);
                b.utf8Value(c.bytes, c.offset, c.length);
            }
        }

        @Override
        public void writeUtf8ValuesWithOffsets(XContentBuilder b, int[] offsetToOrd, Function<BytesRef, BytesRef> converter)
            throws IOException {
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
        }
    }

    private final SortedDocValues offsetDocValues;
    private final SortedValuesSource valueDocValues;
    private final Function<BytesRef, BytesRef> converter;
    private final ByteArrayStreamInput scratch = new ByteArrayStreamInput();

    private boolean hasValue;
    private boolean hasOffset;
    private int[] offsetToOrd;

    ValuesWithOffsetsDocValuesLoader(
        SortedDocValues offsetDocValues,
        SortedValuesSource valueDocValues,
        Function<BytesRef, BytesRef> converter
    ) {
        this.offsetDocValues = offsetDocValues;
        this.valueDocValues = valueDocValues;
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
            valueDocValues.writeUtf8ValuesWithOffsets(b, offsetToOrd, converter);
        } else if (offsetToOrd != null) {
            // in case all values are NULLs
            for (int offset : offsetToOrd) {
                assert offset == -1;
                b.nullValue();
            }
        } else {
            valueDocValues.writeSequentialUtf8Values(b, converter);
        }
    }

}
