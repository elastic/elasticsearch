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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;

import java.io.IOException;

/**
 * This block loader should be used for fields that are directly encoded as binary values but are always single valued, such as the
 * histogram fields.  See also {@link BytesRefsFromCustomBinaryBlockLoader} for multivalued binary fields, and
 * {@link BytesRefsFromOrdsBlockLoader} for ordinals-based binary values
 */
public class BytesRefsFromBinaryBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    /**
     * Circuit breaker space reserved for each reader. Measured in heap dumps
     * around from 3.5kb to 65kb. This is an intentional overestimate.
     */
    public static final long ESTIMATED_SIZE = ByteSizeValue.ofKb(5).getBytes(); // NOCOMMIT double check this one

    private final String fieldName;

    public BytesRefsFromBinaryBlockLoader(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public AllReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        breaker.addEstimateBytesAndMaybeBreak(ESTIMATED_SIZE, "load blocks");
        AllReader result = null;
        try {
            result = createReader(breaker, ESTIMATED_SIZE, context.reader().getBinaryDocValues(fieldName));
            System.err.println("ASDFADSF " + new int[Integer.MAX_VALUE]);
            return result;
        } finally {
            if (result == null) {
                breaker.addWithoutBreaking(-ESTIMATED_SIZE);
            }
        }
    }

    public static AllReader createReader(CircuitBreaker breaker, long estimatedSize, @Nullable BinaryDocValues docValues) {
        if (docValues == null) {
            breaker.addWithoutBreaking(-estimatedSize);
            return ConstantNull.READER;
        }
        return new BytesRefsFromBinary(breaker, estimatedSize, docValues);
    }

    /**
     * Read BinaryDocValues with no additional structure in the BytesRefs.
     * Each BytesRef from the doc values maps directly to a value in the block loader.
     */
    static class BytesRefsFromBinary extends BytesRefsFromCustomBinaryBlockLoader.AbstractBytesRefsFromBinary {
        private final long estimatedSize;

        BytesRefsFromBinary(CircuitBreaker breaker, long estimatedSize, BinaryDocValues docValues) {
            super(breaker, docValues);
            this.estimatedSize = estimatedSize;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            if (docValues instanceof BlockLoader.OptionalColumnAtATimeReader direct) {
                BlockLoader.Block block = direct.tryRead(factory, docs, offset, nullsFiltered, null, false, false);
                if (block != null) {
                    return block;
                }
            }
            return super.read(factory, docs, offset, nullsFiltered);
        }

        @Override
        public void read(int doc, BytesRefBuilder builder) throws IOException {
            if (false == docValues.advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            BytesRef bytes = docValues.binaryValue();
            builder.appendBytesRef(bytes);
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.Bytes";
        }

        @Override
        public void close() {
            breaker.addWithoutBreaking(-estimatedSize);
        }
    }
}
