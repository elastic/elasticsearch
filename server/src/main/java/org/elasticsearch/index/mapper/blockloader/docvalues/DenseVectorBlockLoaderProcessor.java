/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.io.IOException;

/**
 * Processes vector values from doc values and appends them to a builder.
 *
 * @param <B> The type of builder to append results to
 */
public interface DenseVectorBlockLoaderProcessor<B extends BlockLoader.Builder> {

    /**
     * Creates a builder for the given expected count.
     * @param factory the block factory
     * @param expectedCount the expected number of values
     * @param dimensions the vector dimensions
     * @return the builder
     */
    B createBuilder(BlockLoader.BlockFactory factory, int expectedCount, int dimensions);

    /**
     * Process a float vector and append the result to the builder.
     */
    void process(float[] vector, B builder) throws IOException;

    /**
     * Process a byte vector and append the result to the builder.
     */
    void process(byte[] vector, B builder) throws IOException;

    /**
     * Appends a null value to the builder.
     */
    default void appendNull(B builder) {
        builder.appendNull();
    }

    String name();

    /**
     * Processor that appends raw float vectors to a FloatBuilder as multi values.
     */
    class DenseVectorLoaderProcessor implements DenseVectorBlockLoaderProcessor<BlockLoader.FloatBuilder> {

        @Override
        public BlockLoader.FloatBuilder createBuilder(BlockLoader.BlockFactory factory, int expectedCount, int dimensions) {
            return factory.denseVectors(expectedCount, dimensions);
        }

        @Override
        public void process(float[] vector, BlockLoader.FloatBuilder builder) {
            builder.beginPositionEntry();
            for (float f : vector) {
                builder.appendFloat(f);
            }
            builder.endPositionEntry();
        }

        @Override
        public void process(byte[] vector, BlockLoader.FloatBuilder builder) {
            builder.beginPositionEntry();
            for (byte b : vector) {
                builder.appendFloat(b);
            }
            builder.endPositionEntry();
        }

        @Override
        public String name() {
            return "Load";
        }
    }

    /**
     * Processor that calculates similarity scores and appends them to a DoubleBuilder.
     */
    class DenseVectorSimilarityProcessor implements DenseVectorBlockLoaderProcessor<BlockLoader.DoubleBuilder> {
        private final DenseVectorFieldMapper.VectorSimilarityFunctionConfig config;

        public DenseVectorSimilarityProcessor(DenseVectorFieldMapper.VectorSimilarityFunctionConfig config) {
            this.config = config;
        }

        @Override
        public BlockLoader.DoubleBuilder createBuilder(BlockLoader.BlockFactory factory, int expectedCount, int dimensions) {
            return factory.doubles(expectedCount);
        }

        @Override
        public void process(float[] vector, BlockLoader.DoubleBuilder builder) {
            double similarity = config.similarityFunction().calculateSimilarity(vector, config.vector());
            builder.appendDouble(similarity);
        }

        @Override
        public void process(byte[] vector, BlockLoader.DoubleBuilder builder) {
            double similarity = config.similarityFunction().calculateSimilarity(vector, config.vectorAsBytes());
            builder.appendDouble(similarity);
        }

        @Override
        public String name() {
            return config.similarityFunction().function().toString();
        }
    }
}
