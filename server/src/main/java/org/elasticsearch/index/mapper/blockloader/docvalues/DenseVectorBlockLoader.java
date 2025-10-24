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
import org.apache.lucene.index.NumericDocValues;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.io.IOException;

/**
 * Unified block loader for dense vector fields that can output either raw vectors or
 * processed values (like similarity scores) depending on the VectorProcessor provided.
 *
 * @param <B> The type of builder used (FloatBuilder for vectors, DoubleBuilder for scores, etc.)
 */
public class DenseVectorBlockLoader<B extends BlockLoader.Builder> extends AbstractDenseVectorBlockLoader {
    private final int dimensions;
    private final VectorProcessor<B> processor;

    public DenseVectorBlockLoader(
        String fieldName,
        int dimensions,
        DenseVectorFieldMapper.DenseVectorFieldType fieldType,
        VectorProcessor<B> processor
    ) {
        super(fieldName, fieldType);
        this.dimensions = dimensions;
        this.processor = processor;
    }


    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return processor.createBuilder(factory, expectedCount, dimensions);
    }

    @Override
    protected AllReader createFloatReader(FloatVectorValues vectorValues) {
        return new FloatDenseVectorValuesBlockReader<>(vectorValues, dimensions, processor);
    }

    @Override
    protected AllReader createNormalizedFloatReader(FloatVectorValues vectorValues, NumericDocValues magnitudeDocValues) {
        return new FloatDenseVectorNormalizedValuesBlockReader<>(vectorValues, dimensions, processor, magnitudeDocValues);
    }

    @Override
    protected AllReader createByteReader(ByteVectorValues vectorValues) {
        return new ByteDenseVectorValuesBlockReader<>(vectorValues, dimensions, processor);
    }

    @Override
    protected AllReader createBitReader(ByteVectorValues vectorValues) {
        return new BitDenseVectorValuesBlockReader<>(vectorValues, dimensions, processor);
    }

    private static class FloatDenseVectorValuesBlockReader<B extends BlockLoader.Builder> extends
        AbstractVectorValuesReader<FloatVectorValues, B> {

        FloatDenseVectorValuesBlockReader(
            FloatVectorValues floatVectorValues,
            int dimensions,
            VectorProcessor<B> processor
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
            return "BlockDocValuesReader.FloatDenseVectorValuesBlockReader";
        }
    }

    private static class FloatDenseVectorNormalizedValuesBlockReader<B extends BlockLoader.Builder> extends
        FloatDenseVectorValuesBlockReader<B> {
        private final VectorNormalizer normalizer;

        FloatDenseVectorNormalizedValuesBlockReader(
            FloatVectorValues floatVectorValues,
            int dimensions,
            VectorProcessor<B> processor,
            NumericDocValues magnitudeDocValues
        ) {
            super(floatVectorValues, dimensions, processor);
            this.normalizer = new VectorNormalizer(magnitudeDocValues);
        }

        @Override
        protected void processCurrentVector(B builder) throws IOException {
            assertDimensions();
            float[] vector = vectorValues.vectorValue(iterator.index());
            vector = normalizer.normalize(vector, iterator.docID());
            processor.process(vector, builder);
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.FloatDenseVectorNormalizedValuesBlockReader";
        }
    }

    private static class ByteDenseVectorValuesBlockReader<B extends BlockLoader.Builder> extends
        AbstractVectorValuesReader<ByteVectorValues, B> {

        ByteDenseVectorValuesBlockReader(
            ByteVectorValues byteVectorValues,
            int dimensions,
            VectorProcessor<B> processor
        ) {
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
            return "BlockDocValuesReader.ByteDenseVectorValuesBlockReader";
        }
    }

    private static class BitDenseVectorValuesBlockReader<B extends BlockLoader.Builder> extends
        ByteDenseVectorValuesBlockReader<B> {

        BitDenseVectorValuesBlockReader(
            ByteVectorValues byteVectorValues,
            int dimensions,
            VectorProcessor<B> processor
        ) {
            super(byteVectorValues, dimensions, processor);
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
