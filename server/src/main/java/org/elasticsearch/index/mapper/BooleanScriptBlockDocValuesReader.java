/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.script.BooleanFieldScript;

import java.io.IOException;

/**
 * {@link BlockDocValuesReader} implementation for {@code boolean} scripts.
 */
public class BooleanScriptBlockDocValuesReader extends BlockDocValuesReader {
    static class BooleanScriptBlockLoader extends DocValuesBlockLoader {
        private final BooleanFieldScript.LeafFactory factory;

        BooleanScriptBlockLoader(BooleanFieldScript.LeafFactory factory) {
            this.factory = factory;
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.booleans(expectedCount);
        }

        @Override
        public AllReader reader(LeafReaderContext context) throws IOException {
            return new BooleanScriptBlockDocValuesReader(factory.newInstance(context));
        }
    }

    private final BooleanFieldScript script;
    private int docId;

    BooleanScriptBlockDocValuesReader(BooleanFieldScript script) {
        this.script = script;
    }

    @Override
    public int docId() {
        return docId;
    }

    @Override
    public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs) throws IOException {
        // Note that we don't emit falses before trues so we conform to the doc values contract and can use booleansFromDocValues
        try (BlockLoader.BooleanBuilder builder = factory.booleans(docs.count())) {
            for (int i = 0; i < docs.count(); i++) {
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
}
