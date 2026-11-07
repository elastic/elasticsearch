/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.escf.EscfColumnData;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.fielddata.MultiValuedSortableBinaryDocValues;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

public class BinaryDocValuesSyntheticFieldLoaderLayer implements CompositeSyntheticFieldLoader.DocValuesLayer {

    private final String name;
    private final IndexVersion indexVersion;
    private final Function<BytesRef, BytesRef> converter;
    private SortableBinaryDocValues bytesValues;
    private boolean hasValue;

    public BinaryDocValuesSyntheticFieldLoaderLayer(String name, IndexVersion indexVersion) {
        this(name, indexVersion, Function.identity());
    }

    /**
     * @param converter converts each value before it is written as a utf8 string. Doc values that are not directly serializable as
     *                  utf8 (for example the {@code IpFieldMapper} encoded form) supply a converter; others use the identity. A
     *                  converter rather than a subclass keeps the layer's class exact, which is what the direct {@code columnar_stored}
     *                  path checks before it trusts {@link #singleValueSourceLayer} to reproduce this layer's output.
     */
    public BinaryDocValuesSyntheticFieldLoaderLayer(String name, IndexVersion indexVersion, Function<BytesRef, BytesRef> converter) {
        this.name = name;
        this.indexVersion = indexVersion;
        this.converter = Objects.requireNonNull(converter);
    }

    @Override
    public long valueCount() {
        return hasValue ? bytesValues.docValueCount() : 0;
    }

    @Override
    public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
        var docValues = leafReader.getBinaryDocValues(name);
        if (docValues == null) {
            bytesValues = null;
            hasValue = false;
            return null;
        }

        // Pre-DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES indices may use the deprecated IntegratedCounts format, which
        // fromMultiValued() handles as a fallback when the .counts field is absent.
        bytesValues = indexVersion.onOrAfter(IndexVersions.DEPRECATE_INTEGRATED_COUNTS_BINARY_DOC_VALUES)
            ? MultiValuedSortableBinaryDocValues.from(leafReader, name)
            : MultiValuedSortableBinaryDocValues.fromMultiValued(leafReader, name, docValues);

        return docId -> {
            hasValue = bytesValues.advanceExact(docId);
            return hasValue;
        };
    }

    @Override
    public boolean hasValue() {
        return hasValue;
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        if (hasValue == false) {
            return;
        }

        for (int i = 0; i < bytesValues.docValueCount(); ++i) {
            BytesRef value = bytesValues.nextValue();
            writeValue(b, value);
        }
    }

    /**
     * Write a single value to the builder, after the converter. Subclasses can override to change the write format.
     */
    protected void writeValue(XContentBuilder b, BytesRef value) throws IOException {
        final BytesRef converted = converter.apply(value);
        b.utf8Value(converted.bytes, converted.offset, converted.length);
    }

    /**
     * The direct counterpart of this layer on the columnar batch path for a column that holds at most one value per document, which
     * the {@code SeparateCount} layout stores raw: it reads that value from {@code valueColumn}, the column the mapper has just built,
     * and writes it through the same {@link #writeValue} as the loader does. The mapper must write a count of one for every document
     * in the column, or no counts column at all (the {@code PlainBinary} reading); the batch path's fallback and single-valued
     * columns do, and refuse a second value, which is what makes this read exact.
     *
     * @param valueColumn the scalar binary column holding the raw value per document that has one
     */
    public SourceColumn.Layer singleValueSourceLayer(EscfColumnData valueColumn) {
        return new SingleValueSourceLayer(valueColumn);
    }

    private final class SingleValueSourceLayer implements SourceColumn.Layer {
        private final ForwardBytesRefColumn values;
        private boolean present;

        SingleValueSourceLayer(EscfColumnData valueColumn) {
            this.values = new ForwardBytesRefColumn(valueColumn);
        }

        @Override
        public void advance(int doc) {
            present = values.advance(doc);
        }

        @Override
        public long valueCount() {
            return present ? 1 : 0;
        }

        @Override
        public void write(XContentBuilder b) throws IOException {
            if (present) {
                writeValue(b, values.value());
            }
        }
    }

    @Override
    public String fieldName() {
        return name;
    }
}
