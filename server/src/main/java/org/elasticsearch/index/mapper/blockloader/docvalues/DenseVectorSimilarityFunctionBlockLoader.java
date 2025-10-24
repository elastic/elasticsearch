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
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.io.IOException;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.COSINE_MAGNITUDE_FIELD_SUFFIX;

/**
 * {@link BlockLoader} for {@link MappedFieldType.BlockLoaderFunctionConfig} that use a
 * {@link org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.VectorSimilarityFunctionConfig} configuration.
 * It extracts vector from doc values and computes similarity as a result.
 */
public class DenseVectorSimilarityFunctionBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {

    private final String fieldName;
    private final DenseVectorFieldMapper.DenseVectorFieldType fieldType;
    private final DenseVectorFieldMapper.VectorSimilarityFunctionConfig similarityConfig;

    public DenseVectorSimilarityFunctionBlockLoader(
        String fieldName,
        DenseVectorFieldMapper.DenseVectorFieldType fieldType,
        DenseVectorFieldMapper.VectorSimilarityFunctionConfig similarityConfig
    ) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.similarityConfig = similarityConfig;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.doubles(expectedCount);
    }

    @Override
    public AllReader reader(LeafReaderContext context) throws IOException {
        switch (fieldType.getElementType()) {
            case FLOAT -> {
                FloatVectorValues floatVectorValues = context.reader().getFloatVectorValues(fieldName);
                if (floatVectorValues != null) {
                    if (fieldType.isNormalized()) {
                        // Wrap in a normalizing loader
                        NumericDocValues magnitudeDocValues = context.reader()
                            .getNumericDocValues(fieldType.name() + COSINE_MAGNITUDE_FIELD_SUFFIX);
                        return new FloatNormalizedSimilarityFunctionReader(floatVectorValues, similarityConfig, magnitudeDocValues);
                    }
                    return new FloatDenseVectorSimilarityFunctionReader(floatVectorValues, similarityConfig);
                }
            }
            case BYTE -> {
                ByteVectorValues byteVectorValues = context.reader().getByteVectorValues(fieldName);
                if (byteVectorValues != null) {
                    return new ByteDenseVectorFunctionLoaderValueFunctionReader(byteVectorValues, similarityConfig);
                }
            }
            default -> throw new IllegalArgumentException("Unsupported element type: " + fieldType.getElementType());
        }

        return new ConstantNullsReader();
    }

    private abstract static class DenseVectorSimilarityFunctionReader<T extends KnnVectorValues, U> extends BlockDocValuesReader {
        protected final T vectorValues;
        protected final KnnVectorValues.DocIndexIterator iterator;
        protected final DenseVectorFieldMapper.VectorSimilarityFunctionConfig config;

        DenseVectorSimilarityFunctionReader(T vectorValues, DenseVectorFieldMapper.VectorSimilarityFunctionConfig config) {
            this.vectorValues = vectorValues;
            iterator = vectorValues.iterator();
            this.config = config;
        }

        @Override
        public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
            throws IOException {
            try (BlockLoader.DoubleBuilder builder = factory.doubles(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    read(docs.get(i), builder);
                }
                return builder.build();
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void read(int docId, BlockLoader.StoredFields storedFields, BlockLoader.Builder builder) throws IOException {
            read(docId, (BlockLoader.DoubleBuilder) builder);
        }

        private void read(int doc, BlockLoader.DoubleBuilder builder) throws IOException {
            if (iterator.docID() > doc) {
                builder.appendNull();
            } else if (iterator.docID() == doc || iterator.advance(doc) == doc) {
                readValue(builder);
            } else {
                builder.appendNull();
            }
        }

        protected abstract void readValue(BlockLoader.DoubleBuilder builder) throws IOException;

        @Override
        public int docId() {
            return iterator.docID();
        }
    }

    private static class FloatDenseVectorSimilarityFunctionReader extends DenseVectorSimilarityFunctionReader<FloatVectorValues, float[]> {

        FloatDenseVectorSimilarityFunctionReader(
            FloatVectorValues vectorValues,
            DenseVectorFieldMapper.VectorSimilarityFunctionConfig config
        ) {
            super(vectorValues, config);
        }

        @Override
        protected void readValue(BlockLoader.DoubleBuilder builder) throws IOException {
            float[] floats = vectorValues.vectorValue(iterator.index());
            builder.appendDouble(config.similarityFunction().calculateSimilarity(floats, config.vector()));
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.FloatDenseVectorSimilarityFunctionReader";
        }
    }

    private static class FloatNormalizedSimilarityFunctionReader extends FloatDenseVectorSimilarityFunctionReader {

        private final NumericDocValues magnitudeDocValues;

        FloatNormalizedSimilarityFunctionReader(
            FloatVectorValues vectorValues,
            DenseVectorFieldMapper.VectorSimilarityFunctionConfig blockValueLoader,
            NumericDocValues magnitudeDocValues
        ) {
            super(vectorValues, blockValueLoader);
            this.magnitudeDocValues = magnitudeDocValues;
        }

        @Override
        protected void readValue(BlockLoader.DoubleBuilder builder) throws IOException {
            float[] floats = vectorValues.vectorValue(iterator.index());
            // If all vectors are normalized, no doc values will be present. The vector may be normalized already, so we may not have a
            // stored magnitude for all docs
            if ((magnitudeDocValues != null) && magnitudeDocValues.advanceExact(iterator.docID())) {
                float magnitude = Float.intBitsToFloat((int) magnitudeDocValues.longValue());
                for (int i = 0; i < floats.length; i++) {
                    floats[i] *= magnitude;
                }
            }
            builder.appendDouble(config.similarityFunction().calculateSimilarity(floats, config.vector()));
        }
    }

    private static class ByteDenseVectorFunctionLoaderValueFunctionReader extends DenseVectorSimilarityFunctionReader<
        ByteVectorValues,
        byte[]> {

        ByteDenseVectorFunctionLoaderValueFunctionReader(
            ByteVectorValues vectorValues,
            DenseVectorFieldMapper.VectorSimilarityFunctionConfig config
        ) {
            super(vectorValues, config);
        }

        @Override
        protected void readValue(BlockLoader.DoubleBuilder builder) throws IOException {
            byte[] bytes = vectorValues.vectorValue(iterator.index());
            builder.appendDouble(config.similarityFunction().calculateSimilarity(bytes, config.vectorAsBytes()));
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.ByteDenseVectorFunctionLoaderValueFunctionReader";
        }
    }
}
