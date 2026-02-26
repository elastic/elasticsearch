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
public class BooleanScriptBlockDocValuesReader extends BlockScriptReader {
    public static class BooleanScriptBlockLoader extends ScriptBlockLoader {
        private final BooleanFieldScript.LeafFactory factory;
        private final long byteSize;

        public BooleanScriptBlockLoader(BooleanFieldScript.LeafFactory factory, ByteSizeValue byteSize) {
            super(byteSize);
            this.factory = factory;
            this.byteSize = byteSize.getBytes();
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.booleans(expectedCount);
        }

        @Override
        public BlockScriptReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
            return new BooleanScriptBlockDocValuesReader(breaker, factory.newInstance(context), byteSize);
        }
    }

    private final BooleanFieldScript script;
    private int docId;

    BooleanScriptBlockDocValuesReader(CircuitBreaker breaker, BooleanFieldScript script, long byteSize) {
        super(breaker, byteSize);
        this.script = script;
    }

    @Override
    public int docId() {
        return docId;
    }

    @Override
    public void read(int docId, BlockLoader.StoredFields storedFields, BlockLoader.Builder b) throws IOException {
        BlockLoader.BooleanBuilder builder = (BlockLoader.BooleanBuilder) b;
        this.docId = docId;
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
}
