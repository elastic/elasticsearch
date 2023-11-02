/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.script.BooleanFieldScript;

/**
 * {@link BlockDocValuesReader} implementation for {@code boolean} scripts.
 */
public class BooleanScriptBlockDocValuesReader extends BlockDocValuesReader {
    public static BlockLoader blockLoader(BooleanFieldScript.LeafFactory factory) {
        return context -> new BooleanScriptBlockDocValuesReader(factory.newInstance(context));
    }

    private final BooleanFieldScript script;
    private int docId;

    BooleanScriptBlockDocValuesReader(BooleanFieldScript script) {
        this.script = script;
    }

    @Override
    public int docID() {
        return docId;
    }

    @Override
    public BlockLoader.BooleanBuilder builder(BlockLoader.BuilderFactory factory, int expectedCount) {
        // Note that we don't emit falses before trues so we conform to the doc values contract and can use booleansFromDocValues
        return factory.booleansFromDocValues(expectedCount);
    }

    @Override
    public BlockLoader.Block readValues(BlockLoader.BuilderFactory factory, BlockLoader.Docs docs) {
        try (BlockLoader.BooleanBuilder builder = builder(factory, docs.count())) {
            for (int i = 0; i < docs.count(); i++) {
                read(docs.get(i), builder);
            }
            return builder.build();
        }
    }

    @Override
    public void readValuesFromSingleDoc(int docId, BlockLoader.Builder builder) {
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
