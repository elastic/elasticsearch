/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.script.DoubleFieldScript;

import java.io.IOException;

/**
 * {@link BlockDocValuesReader} implementation for {@code double} scripts.
 */
public class DoubleScriptBlockDocValuesReader extends BlockDocValuesReader {
    static class DoubleScriptBlockLoader extends DocValuesBlockLoader {
        private final DoubleFieldScript.LeafFactory factory;

        DoubleScriptBlockLoader(DoubleFieldScript.LeafFactory factory) {
            this.factory = factory;
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.doubles(expectedCount);
        }

        @Override
        public AllReader reader(LeafReaderContext context) throws IOException {
            return new DoubleScriptBlockDocValuesReader(factory.newInstance(context));
        }
    }

    private final DoubleFieldScript script;
    private int docId;

    DoubleScriptBlockDocValuesReader(DoubleFieldScript script) {
        this.script = script;
    }

    @Override
    public int docId() {
        return docId;
    }

    @Override
    public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs) throws IOException {
        // Note that we don't sort the values sort, so we can't use factory.doublesFromDocValues
        try (BlockLoader.DoubleBuilder builder = factory.doubles(docs.count())) {
            for (int i = 0; i < docs.count(); i++) {
                read(docs.get(i), builder);
            }
            return builder.build();
        }
    }

    @Override
    public void read(int docId, BlockLoader.StoredFields storedFields, BlockLoader.Builder builder) throws IOException {
        this.docId = docId;
        read(docId, (BlockLoader.DoubleBuilder) builder);
    }

    private void read(int docId, BlockLoader.DoubleBuilder builder) {
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
