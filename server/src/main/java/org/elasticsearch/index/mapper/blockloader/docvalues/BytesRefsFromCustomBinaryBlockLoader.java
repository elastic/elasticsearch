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
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.index.mapper.BinaryFieldMapper;
import org.elasticsearch.index.mapper.BlockLoader;

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
    public AllReader reader(LeafReaderContext context) throws IOException {
        BinaryDocValues docValues = context.reader().getBinaryDocValues(fieldName);
        if (docValues == null) {
            return new ConstantNullsReader();
        }
        return new BytesRefsFromCustomBinary(docValues);
    }

    public abstract static class AbstractBytesRefsFromBinary extends BlockDocValuesReader {
        protected final BinaryDocValues docValues;

        public AbstractBytesRefsFromBinary(BinaryDocValues docValues) {
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
            return docValues.docID();
        }

        public abstract void read(int docId, BytesRefBuilder builder) throws IOException;
    }

    /**
     * Read BinaryDocValues encoded by {@link BinaryFieldMapper.CustomBinaryDocValuesField}
     */
    static class BytesRefsFromCustomBinary extends AbstractBytesRefsFromBinary {
        private final ByteArrayStreamInput in = new ByteArrayStreamInput();
        private final BytesRef scratch = new BytesRef();

        BytesRefsFromCustomBinary(BinaryDocValues docValues) {
            super(docValues);
        }

        @Override
        public void read(int doc, BytesRefBuilder builder) throws IOException {
            if (false == docValues.advanceExact(doc)) {
                builder.appendNull();
                return;
            }
            BytesRef bytes = docValues.binaryValue();
            assert bytes.length > 0;
            in.reset(bytes.bytes, bytes.offset, bytes.length);
            int count = in.readVInt();
            scratch.bytes = bytes.bytes;

            if (count == 1) {
                scratch.length = in.readVInt();
                scratch.offset = in.getPosition();
                builder.appendBytesRef(scratch);
                return;
            }
            builder.beginPositionEntry();
            for (int v = 0; v < count; v++) {
                scratch.length = in.readVInt();
                scratch.offset = in.getPosition();
                in.setPosition(scratch.offset + scratch.length);
                builder.appendBytesRef(scratch);
            }
            builder.endPositionEntry();
        }

        @Override
        public String toString() {
            return "BlockDocValuesReader.BytesCustom";
        }
    }
}
