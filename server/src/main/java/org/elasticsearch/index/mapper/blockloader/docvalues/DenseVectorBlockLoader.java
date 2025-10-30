/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.io.IOException;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.COSINE_MAGNITUDE_FIELD_SUFFIX;

public class DenseVectorBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    private final String fieldName;
    private final int dimensions;
    private final DenseVectorFieldMapper.DenseVectorFieldType fieldType;

    public DenseVectorBlockLoader(String fieldName, int dimensions, DenseVectorFieldMapper.DenseVectorFieldType fieldType) {
        this.fieldName = fieldName;
        this.dimensions = dimensions;
        this.fieldType = fieldType;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.denseVectors(expectedCount, dimensions);
    }

    @Override
    public AllReader reader(LeafReaderContext context) throws IOException {
        switch (fieldType.getElementType()) {
            case FLOAT -> {
                FloatVectorValues floatVectorValues = context.reader().getFloatVectorValues(fieldName);
                if (floatVectorValues != null) {
                    if (fieldType.isNormalized()) {
                        NumericDocValues magnitudeDocValues = context.reader()
                            .getNumericDocValues(fieldType.name() + COSINE_MAGNITUDE_FIELD_SUFFIX);
                        return new FloatDenseVectorNormalizedValuesBlockReader(floatVectorValues, dimensions, magnitudeDocValues);
                    }
                    return new FloatDenseVectorValuesBlockReader(floatVectorValues, dimensions);
                }
            }
            case BYTE -> {
                ByteVectorValues byteVectorValues = context.reader().getByteVectorValues(fieldName);
                if (byteVectorValues != null) {
                    return new ByteDenseVectorValuesBlockReader(byteVectorValues, dimensions);
                }
            }
            case BIT -> {
                ByteVectorValues byteVectorValues = context.reader().getByteVectorValues(fieldName);
                if (byteVectorValues != null) {
                    return new BitDenseVectorValuesBlockReader(byteVectorValues, dimensions);
                }
            }
        }

        return new ConstantNullsReader();
    }

    private abstract static class DenseVectorValuesBlockReader<T extends KnnVectorValues> extends BlockDocValuesReader {
        protected final T vectorValues;
        protected final KnnVectorValues.DocIndexIterator iterator;
        protected final int dimensions;

        DenseVectorValuesBlockReader(T vectorValues, int dimensions) {
            this.vectorValues = vectorValues;
            iterator = vectorValues.iterator();
            this.dimensions = dimensions;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            // Doubles from doc values ensures that the values are in order
            try (BlockLoader.FloatBuilder builder = factory.denseVectors(docs.count() - offset, dimensions)) {
                for (int i = offset; i < docs.count(); i++) {
                    read(docs.get(i), builder);
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (BlockLoader.FloatBuilder) builder);
        }

        private void read(int doc, BlockLoader.FloatBuilder builder) throws IOException {
            assertDimensions();

            if (iterator.docID() > doc) {
                builder.appendNull();
            } else if (iterator.docID() == doc || iterator.advance(doc) == doc) {
                builder.beginPositionEntry();
                appendDoc(builder);
                builder.endPositionEntry();
            } else {
                builder.appendNull();
            }
        }

        protected abstract void appendDoc(BlockLoader.FloatBuilder builder) throws IOException;

        @Override
        public int docId() {
            return iterator.docID();
        }

        protected void assertDimensions() {
            assert vectorValues.dimension() == dimensions
                : "unexpected dimensions for vector value; expected " + dimensions + " but got " + vectorValues.dimension();
        }
    }

    private static class FloatDenseVectorValuesBlockReader extends DenseVectorValuesBlockReader<FloatVectorValues> {

        FloatDenseVectorValuesBlockReader(FloatVectorValues floatVectorValues, int dimensions) {
            super(floatVectorValues, dimensions);
        }

        protected void appendDoc(BlockLoader.FloatBuilder builder) throws IOException {
            float[] floats = vectorValues.vectorValue(iterator.index());
            for (float aFloat : floats) {
                builder.appendFloat(aFloat);
            }
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.FloatDenseVectorValuesBlockReader";
        }
    }

    private static class FloatDenseVectorNormalizedValuesBlockReader extends DenseVectorValuesBlockReader<FloatVectorValues> {
        private final NumericDocValues magnitudeDocValues;

        FloatDenseVectorNormalizedValuesBlockReader(
            FloatVectorValues floatVectorValues,
            int dimensions,
            NumericDocValues magnitudeDocValues
        ) {
            super(floatVectorValues, dimensions);
            this.magnitudeDocValues = magnitudeDocValues;
        }

        @Override
        protected void appendDoc(BlockLoader.FloatBuilder builder) throws IOException {
            float magnitude = 1.0f;
            // If all vectors are normalized, no doc values will be present. The vector may be normalized already, so we may not have a
            // stored magnitude for all docs
            if ((magnitudeDocValues != null) && magnitudeDocValues.advanceExact(iterator.docID())) {
                magnitude = Float.intBitsToFloat((int) magnitudeDocValues.longValue());
            }
            float[] floats = vectorValues.vectorValue(iterator.index());
            for (float aFloat : floats) {
                builder.appendFloat(aFloat * magnitude);
            }
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.FloatDenseVectorNormalizedValuesBlockReader";
        }
    }

    private static class ByteDenseVectorValuesBlockReader extends DenseVectorValuesBlockReader<ByteVectorValues> {
        ByteDenseVectorValuesBlockReader(ByteVectorValues floatVectorValues, int dimensions) {
            super(floatVectorValues, dimensions);
        }

        protected void appendDoc(BlockLoader.FloatBuilder builder) throws IOException {
            byte[] bytes = vectorValues.vectorValue(iterator.index());
            for (byte aFloat : bytes) {
                builder.appendFloat(aFloat);
            }
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.ByteDenseVectorValuesBlockReader";
        }
    }

    private static class BitDenseVectorValuesBlockReader extends ByteDenseVectorValuesBlockReader {

        BitDenseVectorValuesBlockReader(ByteVectorValues floatVectorValues, int dimensions) {
            super(floatVectorValues, dimensions);
        }

        @Override
        protected void assertDimensions() {
            assert vectorValues.dimension() * Byte.SIZE == dimensions
                : "unexpected dimensions for vector value; expected " + dimensions + " but got " + vectorValues.dimension() * Byte.SIZE;
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.BitDenseVectorValuesBlockReader";
        }
    }
}
