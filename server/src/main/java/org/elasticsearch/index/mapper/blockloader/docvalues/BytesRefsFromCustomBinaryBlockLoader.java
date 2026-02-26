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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.index.mapper.BinaryFieldMapper;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.blockloader.docvalues.tracking.TrackingBinaryDocValues;

import java.io.IOException;

/**
 * This block loader should be used for "wildcard-style" binary values, which is to say fields we have encoded into a binary
 * format that supports multivalued via an encoding on our side.  See also {@link BytesRefsFromOrdsBlockLoader} for ordinals
 * based multivalue aware binary fields, and {@link BytesRefsFromBinaryBlockLoader} for single-valued binary fields.
 */
public class BytesRefsFromCustomBinaryBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {
    private final String fieldName;

    public BytesRefsFromCustomBinaryBlockLoader(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public AllReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        TrackingBinaryDocValues dv = TrackingBinaryDocValues.get(breaker, context, fieldName);
        if (dv == null) {
            return ConstantNull.READER;
        }
        return new BytesRefsFromCustomBinary(dv);
    }

    public abstract static class AbstractBytesRefsFromBinary extends BlockDocValuesReader {
        protected final TrackingBinaryDocValues docValues;

        public AbstractBytesRefsFromBinary(TrackingBinaryDocValues docValues) {
            super(null);
            this.docValues = docValues;
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            try (BlockLoader.BytesRefBuilder builder = factory.bytesRefs(docs.count() - offset)) {
                for (int i = offset; i < docs.count(); i++) {
                    int doc = docs.get(i);
                    read(doc, builder);
                }
                return builder.build();
            }
        }

        @Override
        public void read(int docId, BlockLoader.StoredFields storedFields, Builder builder) throws IOException {
            read(docId, (BytesRefBuilder) builder);
        }

        @Override
        public int docId() {
            return docValues.docValues().docID();
        }

        public abstract void read(int docId, BytesRefBuilder builder) throws IOException;

        @Override
        public void close() {
            docValues.close();
        }
    }

    /**
     * Read BinaryDocValues encoded by {@link BinaryFieldMapper.CustomBinaryDocValuesField}
     */
    static class BytesRefsFromCustomBinary extends AbstractBytesRefsFromBinary {
        private final CustomBinaryDocValuesReader reader = new CustomBinaryDocValuesReader();

        BytesRefsFromCustomBinary(TrackingBinaryDocValues docValues) {
            super(docValues);
        }

        @Override
        public BlockLoader.Block read(BlockFactory factory, Docs docs, int offset, boolean nullsFiltered) throws IOException {
            if (docValues.docValues() instanceof BlockLoader.OptionalColumnAtATimeReader direct) {
                BlockLoader.Block block = direct.tryRead(factory, docs, offset, nullsFiltered, null, false, true);
                if (block != null) {
                    return block;
                }
            }
            return super.read(factory, docs, offset, nullsFiltered);
        }

        @Override
        public void read(int doc, BytesRefBuilder builder) throws IOException {
            if (false == docValues.docValues().advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            BytesRef bytes = docValues.docValues().binaryValue();
            reader.read(bytes, builder);
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.BytesCustom";
        }
    }
}
