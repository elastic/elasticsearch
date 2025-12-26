/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.VectorEncoderDecoder;

import java.io.IOException;

public class DenseVectorFromBinaryBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    private final String fieldName;
    private final int dims;
    private final IndexVersion indexVersion;
    private final DenseVectorFieldMapper.ElementType elementType;

    public DenseVectorFromBinaryBlockLoader(
        String fieldName,
        int dims,
        IndexVersion indexVersion,
        DenseVectorFieldMapper.ElementType elementType
    ) {
        this.fieldName = fieldName;
        this.dims = dims;
        this.indexVersion = indexVersion;
        this.elementType = elementType;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.denseVectors(expectedCount, dims);
    }

    @Override
    public AllReader reader(LeafReaderContext context) throws IOException {
        BinaryDocValues docValues = context.reader().getBinaryDocValues(fieldName);
        if (docValues == null) {
            return new ConstantNullsReader();
        }
        return switch (elementType) {
            case FLOAT -> new FloatDenseVectorFromBinary(docValues, dims, indexVersion);
            case BFLOAT16 -> new BFloat16DenseVectorFromBinary(docValues, dims, indexVersion);
            case BYTE -> new ByteDenseVectorFromBinary(docValues, dims, indexVersion);
            case BIT -> new BitDenseVectorFromBinary(docValues, dims, indexVersion);
        };
    }

    // Abstract base for dense vector readers
    private abstract static class AbstractDenseVectorFromBinary<T> extends BlockDocValuesReader {
        protected final BinaryDocValues docValues;
        protected final IndexVersion indexVersion;
        protected final int dimensions;
        protected final T scratch;

        AbstractDenseVectorFromBinary(BinaryDocValues docValues, int dims, IndexVersion indexVersion, T scratch) {
            this.docValues = docValues;
            this.indexVersion = indexVersion;
            this.dimensions = dims;
            this.scratch = scratch;
        }

        @Override
        public int docId() {
            return docValues.docID();
        }

        @Override
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (BlockLoader.FloatBuilder) builder);
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (BlockLoader.FloatBuilder builder = factory.denseVectors(docs.count() - offset, dimensions)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        private void read(int doc, BlockLoader.FloatBuilder builder) throws IOException {
            if (docValues.advanceExact(doc) == false) {
                builder.appendNull();
                return;
            }
            BytesRef bytesRef = docValues.binaryValue();
            assert bytesRef.length > 0;
            decodeDenseVector(bytesRef, scratch);

            builder.beginPositionEntry();
            writeScratchToBuilder(scratch, builder);
            builder.endPositionEntry();
        }

        protected abstract void decodeDenseVector(BytesRef bytesRef, T scratch);

        protected abstract void writeScratchToBuilder(T scratch, BlockLoader.FloatBuilder builder);
    }

    private static class FloatDenseVectorFromBinary extends AbstractDenseVectorFromBinary<float[]> {
        FloatDenseVectorFromBinary(BinaryDocValues docValues, int dims, IndexVersion indexVersion) {
            super(docValues, dims, indexVersion, new float[dims]);
        }

        @Override
        protected void writeScratchToBuilder(float[] scratch, BlockLoader.FloatBuilder builder) {
            for (float value : scratch) {
                builder.appendFloat(value);
            }
        }

        @Override
        protected void decodeDenseVector(BytesRef bytesRef, float[] scratch) {
            VectorEncoderDecoder.decodeDenseVector(indexVersion, bytesRef, scratch);
        }

        @Override
        public String toString() {
            return "FloatDenseVectorFromBinary.Bytes";
        }
    }

    private static class BFloat16DenseVectorFromBinary extends AbstractDenseVectorFromBinary<float[]> {
        BFloat16DenseVectorFromBinary(BinaryDocValues docValues, int dims, IndexVersion indexVersion) {
            super(docValues, dims, indexVersion, new float[dims]);
        }

        @Override
        protected void writeScratchToBuilder(float[] scratch, BlockLoader.FloatBuilder builder) {
            for (float value : scratch) {
                builder.appendFloat(value);
            }
        }

        @Override
        protected void decodeDenseVector(BytesRef bytesRef, float[] scratch) {
            VectorEncoderDecoder.decodeBFloat16DenseVector(bytesRef, scratch);
        }

        @Override
        public String toString() {
            return "BFloat16DenseVectorFromBinary.Bytes";
        }
    }

    private static class ByteDenseVectorFromBinary extends AbstractDenseVectorFromBinary<byte[]> {
        ByteDenseVectorFromBinary(BinaryDocValues docValues, int dims, IndexVersion indexVersion) {
            this(docValues, dims, indexVersion, dims);
        }

        protected ByteDenseVectorFromBinary(BinaryDocValues docValues, int dims, IndexVersion indexVersion, int readScratchSize) {
            super(docValues, dims, indexVersion, new byte[readScratchSize]);
        }

        @Override
        public String toString() {
            return "ByteDenseVectorFromBinary.Bytes";
        }

        protected void writeScratchToBuilder(byte[] scratch, BlockLoader.FloatBuilder builder) {
            for (byte value : scratch) {
                builder.appendFloat(value);
            }
        }

        protected void decodeDenseVector(BytesRef bytesRef, byte[] scratch) {
            VectorEncoderDecoder.decodeDenseVector(indexVersion, bytesRef, scratch);
        }
    }

    private static class BitDenseVectorFromBinary extends ByteDenseVectorFromBinary {
        BitDenseVectorFromBinary(BinaryDocValues docValues, int dims, IndexVersion indexVersion) {
            super(docValues, dims, indexVersion, dims / Byte.SIZE);
        }

        @Override
        public String toString() {
            return "BitDenseVectorFromBinary.Bytes";
        }
    }
}
