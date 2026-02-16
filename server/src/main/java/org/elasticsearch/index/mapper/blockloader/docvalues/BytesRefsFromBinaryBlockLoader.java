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
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOFunction;
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

    private final IOFunction<LeafReader, BinaryDocValues> docValuesSupplier;

    public BytesRefsFromBinaryBlockLoader(String fieldName) {
        this(leafReader -> leafReader.getBinaryDocValues(fieldName));
    }

    /**
     * Create a block loader from a {@link BinaryDocValues} supplier.
     * This is useful when the doc values are not directly stored in a single field
     * but are composed of multiple sources, as is the case for Pattern Text.
     */
    public BytesRefsFromBinaryBlockLoader(IOFunction<LeafReader, BinaryDocValues> docValuesSupplier) {
        this.docValuesSupplier = docValuesSupplier;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public AllReader reader(LeafReaderContext context) throws IOException {
        BinaryDocValues docValues = docValuesSupplier.apply(context.reader());
        return createReader(docValues);
    }

    public static AllReader createReader(@Nullable BinaryDocValues docValues) {
        if (docValues == null) {
            return ConstantNull.READER;
        }
        return new BytesRefsFromBinary(docValues);
    }

    /**
     * Read BinaryDocValues with no additional structure in the BytesRefs.
     * Each BytesRef from the doc values maps directly to a value in the block loader.
     */
    public static class BytesRefsFromBinary extends BytesRefsFromCustomBinaryBlockLoader.AbstractBytesRefsFromBinary {

        public BytesRefsFromBinary(BinaryDocValues docValues) {
            super(docValues);
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
    }
}
