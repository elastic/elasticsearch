/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.script;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.script.BooleanFieldScript;

import java.io.IOException;

/**
 * {@link BlockDocValuesReader} implementation for {@code boolean} scripts.
 */
public class BooleanScriptBlockDocValuesReader extends BlockDocValuesReader {
    public static class BooleanScriptBlockLoader extends DocValuesBlockLoader {
        private final BooleanFieldScript.LeafFactory factory;
        private final long byteSize;

        public BooleanScriptBlockLoader(BooleanFieldScript.LeafFactory factory, ByteSizeValue byteSize) {
            this.factory = factory;
            this.byteSize = byteSize.getBytes();
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.booleans(expectedCount);
        }

        @Override
        public AllReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
            breaker.addEstimateBytesAndMaybeBreak(byteSize, "load blocks");
            BooleanFieldScript script = null;
            try {
                script = factory.newInstance(context);
                return new BooleanScriptBlockDocValuesReader(breaker, script, byteSize);
            } finally {
                if (script == null) {
                    breaker.addWithoutBreaking(-byteSize);
                }
            }
        }
    }

    private final BooleanFieldScript script;
    private final long byteSize;
    private int docId;

    BooleanScriptBlockDocValuesReader(CircuitBreaker breaker, BooleanFieldScript script, long byteSize) {
        super(breaker);
        this.script = script;
        this.byteSize = byteSize;
    }

    @Override
    public int docId() {
        return docId;
    }

    @Override
    public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs, int offset, boolean nullsFiltered)
        throws IOException {
        // Note that we don't emit falses before trues so we conform to the doc values contract and can use booleansFromDocValues
        try (BlockLoader.BooleanBuilder builder = factory.booleans(docs.count() - offset)) {
            for (int i = offset; i < docs.count(); i++) {
                read(docs.get(i), builder);
            }
            return builder.build();
        }
    }

    @Override
    public void read(int docId, BlockLoader.StoredFields storedFields, BlockLoader.Builder builder) throws IOException {
        this.docId = docId;
        read(docId, (BlockLoader.BooleanBuilder) builder);
    }

    private void read(int docId, BlockLoader.BooleanBuilder builder) {
        script.runForDoc(docId);
        int total = script.falses() + script.trues();
        switch (total) {
            case 0 -> builder.appendNull();
            case 1 -> builder.appendBoolean(script.trues() > 0);
            default -> {
                builder.beginPositionEntry();
                for (int i = 0; i < script.falses(); i++) {
                    builder.appendBoolean(false);
                }
                for (int i = 0; i < script.trues(); i++) {
                    builder.appendBoolean(true);
                }
                builder.endPositionEntry();
            }
        }
    }

    @Override
    public String toString() {
        return "ScriptBooleans";
    }

    @Override
    public void close() {
        breaker.addWithoutBreaking(-byteSize);
    }
}
