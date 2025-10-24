/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromCustomBinaryBlockLoader;

import java.io.IOException;

public class PatternTextBlockLoader extends BlockDocValuesReader.DocValuesBlockLoader {

    private final PatternTextFieldMapper.DocValuesSupplier docValuesSupplier;

    PatternTextBlockLoader(PatternTextFieldMapper.DocValuesSupplier docValuesSupplier) {
        this.docValuesSupplier = docValuesSupplier;
    }

    @Override
    public BytesRefBuilder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public AllReader reader(LeafReaderContext context) throws IOException {
        var docValues = docValuesSupplier.get(context.reader());
        if (docValues == null) {
            return new ConstantNullsReader();
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
