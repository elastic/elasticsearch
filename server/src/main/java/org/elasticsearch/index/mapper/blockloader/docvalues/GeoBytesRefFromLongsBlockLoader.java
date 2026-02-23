/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.function.Function;

/**
 * This is a GeoPoint-specific block loader that helps deal with an edge case where doc_values are available, yet
 * FieldExtractPreference = NONE. When this happens, the BlockLoader sanity checker (see PlannerUtils.toElementType) expects a BytesRef.
 * This implies that we need to load the value from _source. This however is very slow, especially when synthetic source is enabled.
 * We're better off reading from doc_values and converting to BytesRef to satisfy the checker. This is what this block loader is for.
 */
public final class GeoBytesRefFromLongsBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {

    private final String fieldName;

    public GeoBytesRefFromLongsBlockLoader(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public AllReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        breaker.addEstimateBytesAndMaybeBreak(AbstractLongsFromDocValuesBlockLoader.ESTIMATED_SIZE, "load blocks");
        boolean release = true;
        try {
            SortedNumericDocValues docValues = context.reader().getSortedNumericDocValues(fieldName);
            if (docValues != null) {
                release = false;
                return new BytesRefsFromLong(breaker, docValues, geoPointLong -> {
                    GeoPoint gp = new GeoPoint().resetFromEncoded(geoPointLong);
                    byte[] wkb = WellKnownBinary.toWKB(new Point(gp.getX(), gp.getY()), ByteOrder.LITTLE_ENDIAN);
                    return new BytesRef(wkb);
                });
            }

            return ConstantNull.READER;
        } finally {
            if (release) {
                breaker.addWithoutBreaking(-AbstractLongsFromDocValuesBlockLoader.ESTIMATED_SIZE);
            }
        }
    }

    private static final class BytesRefsFromLong extends BlockDocValuesReader {

        private final SortedNumericDocValues numericDocValues;
        private final Function<Long, BytesRef> longsToBytesRef;

        BytesRefsFromLong(CircuitBreaker breaker, SortedNumericDocValues numericDocValues, Function<Long, BytesRef> longsToBytesRef) {
            super(breaker);
            this.numericDocValues = numericDocValues;
            this.longsToBytesRef = longsToBytesRef;
        }

        @Override
        protected int docId() {
            return numericDocValues.docID();
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.BytesRefsFromLong";
        }

        @Override
        public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
            throws IOException {

            try (BlockLoader.BytesRefBuilder builder = factory.bytesRefs(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, BlockLoader.StoredFields storedFields, BlockLoader.Builder builder) throws IOException {
            read(docId, (BlockLoader.BytesRefBuilder) builder);
        }

        private void read(int doc, BlockLoader.BytesRefBuilder builder) throws IOException {
            // no more values remaining
            if (numericDocValues.advanceExact(doc) == false) {
                builder.appendNull();
                return;
            }
            int count = numericDocValues.docValueCount();
            if (count == 1) {
                BytesRef bytesRefValue = longsToBytesRef.apply(numericDocValues.nextValue());
                builder.appendBytesRef(bytesRefValue);
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                BytesRef bytesRefValue = longsToBytesRef.apply(numericDocValues.nextValue());
                builder.appendBytesRef(bytesRefValue);
            }
            builder.endPositionEntry();
        }

        @Override
        public void close() {
            breaker.addWithoutBreaking(-AbstractLongsFromDocValuesBlockLoader.ESTIMATED_SIZE);
        }
    }
}
