/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.script.DoubleFieldScript;

/**
 * {@link BlockDocValuesReader} implementation for {@code double} scripts.
 */
public class DoubleScriptBlockDocValuesReader extends BlockDocValuesReader {
    public static BlockLoader blockLoader(DoubleFieldScript.LeafFactory factory) {
        return context -> new DoubleScriptBlockDocValuesReader(factory.newInstance(context));
    }

    private final DoubleFieldScript script;
    private int docId;

    DoubleScriptBlockDocValuesReader(DoubleFieldScript script) {
        this.script = script;
    }

    @Override
    public int docID() {
        return docId;
    }

    @Override
    public BlockLoader.DoubleBuilder builder(BlockLoader.BuilderFactory factory, int expectedCount) {
        return factory.doubles(expectedCount);  // Note that we don't pre-sort our output so we can't use doublesFromDocValues
    }

    @Override
    public BlockLoader.Block readValues(BlockLoader.BuilderFactory factory, BlockLoader.Docs docs) {
        try (BlockLoader.DoubleBuilder builder = builder(factory, docs.count())) {
            for (int i = 0; i < docs.count(); i++) {
                read(docs.get(i), builder);
            }
            return builder.build();
        }
    }

    @Override
    public void readValuesFromSingleDoc(int docId, BlockLoader.Builder builder) {
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
