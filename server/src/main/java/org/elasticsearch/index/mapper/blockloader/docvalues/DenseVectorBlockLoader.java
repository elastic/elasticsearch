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

/**
 * Block loader for dense vector fields that can output either raw vectors or
 * processed values (like similarity scores) depending on the {@link DenseVectorBlockLoaderProcessor} provided.
 *
 * @param <B> The type of builder used (FloatBuilder for vectors, DoubleBuilder for scores, etc.)
 */
public class DenseVectorBlockLoader<B extends BlockLoader.Builder> extends BlockDocValuesReader.DocValuesBlockLoader {
    private final String fieldName;
    private final int dimensions;
    private final DenseVectorFieldMapper.DenseVectorFieldType fieldType;
    private final DenseVectorBlockLoaderProcessor<B> processor;

    public DenseVectorBlockLoader(
        String fieldName,
        int dimensions,
        DenseVectorFieldMapper.DenseVectorFieldType fieldType,
        DenseVectorBlockLoaderProcessor<B> processor
    ) {
        this.fieldName = fieldName;
        this.dimensions = dimensions;
        this.fieldType = fieldType;
        this.processor = processor;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return processor.createBuilder(factory, expectedCount, dimensions);
    }

    @Override
    public AllReader reader(LeafReaderContext context) throws IOException {
        switch (fieldType.getElementType()) {
            case FLOAT, BFLOAT16 -> {
                FloatVectorValues floatVectorValues = context.reader().getFloatVectorValues(fieldName);
                if (floatVectorValues != null) {
                    if (fieldType.isNormalized()) {
                        NumericDocValues magnitudeDocValues = context.reader()
                            .getNumericDocValues(fieldType.name() + COSINE_MAGNITUDE_FIELD_SUFFIX);
                        return new FloatDenseVectorNormalizedValuesBlockReader<>(
                            floatVectorValues,
                            dimensions,
                            processor,
                            magnitudeDocValues
                        );
                    }
                    return new FloatDenseVectorValuesBlockReader<>(floatVectorValues, dimensions, processor);
                }
            }
            case BYTE -> {
                ByteVectorValues byteVectorValues = context.reader().getByteVectorValues(fieldName);
                if (byteVectorValues != null) {
                    return new ByteDenseVectorValuesBlockReader<>(byteVectorValues, dimensions, processor);
                }
            }
            case BIT -> {
                ByteVectorValues byteVectorValues = context.reader().getByteVectorValues(fieldName);
                if (byteVectorValues != null) {
                    return new BitDenseVectorValuesBlockReader<>(byteVectorValues, dimensions, processor);
                }
            }
        }

        return new ConstantNullsReader();
    }

    /**
     * Abstract base class for readers that process dense vector values.
     * Provides common iteration and null-handling logic.
     *
     * @param <T> The type of KNN vector values (FloatVectorValues, ByteVectorValues, etc.)
     * @param <B> The type of builder used to construct blocks
     */
    private abstract static class AbstractVectorValuesReader<T extends KnnVectorValues, B extends BlockLoader.Builder> extends
        BlockDocValuesReader {

        protected final T vectorValues;
        protected final KnnVectorValues.DocIndexIterator iterator;
        protected final DenseVectorBlockLoaderProcessor<B> processor;
        protected final int dimensions;

        protected AbstractVectorValuesReader(T vectorValues, DenseVectorBlockLoaderProcessor<B> processor, int dimensions) {
            this.vectorValues = vectorValues;
            this.iterator = vectorValues.iterator();
            this.processor = processor;
            this.dimensions = dimensions;
        }

        @Override
        @SuppressWarnings("unchecked")
        public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
            throws IOException {
            try (B builder = processor.createBuilder(factory, docs.count() - offset, dimensions)) {
                for (int i = offset; i < docs.count(); i++) {
                    read(docs.get(i), builder);
                }
                return builder.build();
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public void read(int docId, BlockLoader.StoredFields storedFields, BlockLoader.Builder builder) throws IOException {
            read(docId, (B) builder);
        }

        /**
         * Reads a document and appends it to the builder.
         * Handles null values when the document doesn't have a vector.
         */
        protected void read(int doc, B builder) throws IOException {
            if (iterator.docID() > doc) {
                processor.appendNull(builder);
            } else if (iterator.docID() == doc || iterator.advance(doc) == doc) {
                processCurrentVector(builder);
            } else {
                processor.appendNull(builder);
            }
        }

        @Override
        public int docId() {
            return iterator.docID();
        }

        /**
         * Retrieves the vector value at the current iterator position and processes it.
         * Subclasses must call the appropriate processor.process() method with the correct type.
         */
        protected abstract void processCurrentVector(B builder) throws IOException;

        protected void assertDimensions() {
            assert vectorValues.dimension() == dimensions
                : "unexpected dimensions for vector value; expected " + dimensions + " but got " + vectorValues.dimension();
        }
    }

    private static class FloatDenseVectorValuesBlockReader<B extends BlockLoader.Builder> extends AbstractVectorValuesReader<
        FloatVectorValues,
        B> {

        FloatDenseVectorValuesBlockReader(
            FloatVectorValues floatVectorValues,
            int dimensions,
            DenseVectorBlockLoaderProcessor<B> processor
        ) {
            super(floatVectorValues, processor, dimensions);
        }

        @Override
        protected void processCurrentVector(B builder) throws IOException {
            assertDimensions();
            float[] vector = vectorValues.vectorValue(iterator.index());
            processor.process(vector, builder);
        }

        @Override
        public String toString() {
            return "FloatDenseVectorFromDocValues." + processor.name();
        }
    }

    private static class FloatDenseVectorNormalizedValuesBlockReader<B extends BlockLoader.Builder> extends
        FloatDenseVectorValuesBlockReader<B> {
        private final NumericDocValues magnitudeDocValues;

        FloatDenseVectorNormalizedValuesBlockReader(
            FloatVectorValues floatVectorValues,
            int dimensions,
            DenseVectorBlockLoaderProcessor<B> processor,
            NumericDocValues magnitudeDocValues
        ) {
            super(floatVectorValues, dimensions, processor);
            this.magnitudeDocValues = magnitudeDocValues;
        }

        @Override
        protected void processCurrentVector(B builder) throws IOException {
            assertDimensions();
            float[] vector = vectorValues.vectorValue(iterator.index());

            // Normalize the vector by multiplying each element by the stored magnitude.
            // If all vectors are normalized or the magnitude is not stored for this doc,
            // the vector remains unchanged.
            if (magnitudeDocValues != null && magnitudeDocValues.advanceExact(iterator.docID())) {
                float magnitude = Float.intBitsToFloat((int) magnitudeDocValues.longValue());
                for (int i = 0; i < vector.length; i++) {
                    vector[i] *= magnitude;
                }
            }

            processor.process(vector, builder);
        }

        @Override
        public String toString() {
            return "FloatDenseVectorFromDocValues.Normalized." + processor.name();
        }
    }

    private static class ByteDenseVectorValuesBlockReader<B extends BlockLoader.Builder> extends AbstractVectorValuesReader<
        ByteVectorValues,
        B> {

        ByteDenseVectorValuesBlockReader(ByteVectorValues byteVectorValues, int dimensions, DenseVectorBlockLoaderProcessor<B> processor) {
            super(byteVectorValues, processor, dimensions);
        }

        @Override
        protected void processCurrentVector(B builder) throws IOException {
            assertDimensions();
            byte[] vector = vectorValues.vectorValue(iterator.index());
            processor.process(vector, builder);
        }

        @Override
        public String toString() {
            return "ByteDenseVectorFromDocValues." + processor.name();
        }
    }

    private static class BitDenseVectorValuesBlockReader<B extends BlockLoader.Builder> extends ByteDenseVectorValuesBlockReader<B> {

        BitDenseVectorValuesBlockReader(ByteVectorValues byteVectorValues, int dimensions, DenseVectorBlockLoaderProcessor<B> processor) {
            super(byteVectorValues, dimensions, processor);
        }

        @Override
        protected void assertDimensions() {
            assert vectorValues.dimension() * Byte.SIZE == dimensions
                : "unexpected dimensions for vector value; expected " + dimensions + " but got " + vectorValues.dimension() * Byte.SIZE;
        }

        @Override
        public String toString() {
            return "BitDenseVectorFromDocValues." + processor.name();
        }
    }
}
