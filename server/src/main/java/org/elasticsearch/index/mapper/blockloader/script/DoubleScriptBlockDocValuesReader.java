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
import org.elasticsearch.script.DoubleFieldScript;

import java.io.IOException;

/**
 * {@link BlockDocValuesReader} implementation for {@code double} scripts.
 */
public class DoubleScriptBlockDocValuesReader extends BlockScriptReader {
    public static class DoubleScriptBlockLoader extends ScriptBlockLoader {
        private final DoubleFieldScript.LeafFactory factory;
        private final long byteSize;

        public DoubleScriptBlockLoader(DoubleFieldScript.LeafFactory factory, ByteSizeValue byteSize) {
            super(byteSize);
            this.factory = factory;
            this.byteSize = byteSize.getBytes();
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.doubles(expectedCount);
        }

        @Override
        public BlockScriptReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
            return new DoubleScriptBlockDocValuesReader(breaker, factory.newInstance(context), byteSize);
        }
    }

    private final DoubleFieldScript script;
    private int docId;

    DoubleScriptBlockDocValuesReader(CircuitBreaker breaker, DoubleFieldScript script, long byteSize) {
        super(breaker, byteSize);
        this.script = script;
    }

    @Override
    public int docId() {
        return docId;
    }

    @Override
    public void read(int docId, BlockLoader.StoredFields storedFields, BlockLoader.Builder b) throws IOException {
        BlockLoader.DoubleBuilder builder = (BlockLoader.DoubleBuilder) b;
        this.docId = docId;
        script.runForDoc(docId);
        switch (script.count()) {
            case 0 -> builder.appendNull();
            case 1 -> builder.appendDouble(script.values()[0]);
            default -> {
                builder.beginPositionEntry();
                for (int i = 0; i < script.count(); i++) {
                    builder.appendDouble(script.values()[i]);
                }
                builder.endPositionEntry();
            }
        }
    }

    @Override
    public String toString() {
        return "ScriptDoubles";
    }
}
