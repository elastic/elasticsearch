/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;

import java.io.IOException;
import java.util.List;

/**
 * Value block loader for {@code shape}/{@code geo_shape} fields in columnar index modes. It reads the geometries kept in
 * the field-owned doc value ({@link GeometrySourceDocValuesField}) and emits their WKB directly, preserving input order.
 */
public class GeometrySourceBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {

    private final String fieldName;

    public GeometrySourceBlockLoader(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public BlockLoader.Builder builder(BlockLoader.BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public BlockLoader.ColumnAtATimeReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        BinaryDocValues docValues = context.reader().getBinaryDocValues(fieldName);
        if (docValues == null) {
            return ConstantNull.COLUMN_READER;
        }
        return new GeometrySourceReader(docValues);
    }

    private static class GeometrySourceReader implements BlockLoader.ColumnAtATimeReader {
        private final BinaryDocValues docValues;

        GeometrySourceReader(BinaryDocValues docValues) {
            this.docValues = docValues;
        }

        @Override
        public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
            throws IOException {
            try (BlockLoader.BytesRefBuilder builder = factory.bytesRefs(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    read(docs.get(i), builder);
                }
                return builder.build();
            }
        }

        private void read(int doc, BlockLoader.BytesRefBuilder builder) throws IOException {
            if (docValues.advanceExact(doc) == false) {
                builder.appendNull();
                return;
            }
            List<BytesRef> wkbs = GeometrySourceDocValuesField.decodeWkb(docValues.binaryValue());
            if (wkbs.size() == 1) {
                builder.appendBytesRef(wkbs.get(0));
                return;
            }
            builder.beginPositionEntry();
            for (BytesRef wkb : wkbs) {
                builder.appendBytesRef(wkb);
            }
            builder.endPositionEntry();
        }

        @Override
        public boolean canReuse(int startingDocID) {
            return true;
        }

        @Override
        public String toString() {
            return "GeometrySource";
        }

        @Override
        public void close() {}
    }
}
