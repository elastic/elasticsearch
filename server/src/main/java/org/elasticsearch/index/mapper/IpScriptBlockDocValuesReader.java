/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.script.IpFieldScript;

/**
 * {@link BlockDocValuesReader} implementation for keyword scripts.
 */
public class IpScriptBlockDocValuesReader extends BlockDocValuesReader {
    public static BlockLoader blockLoader(IpFieldScript.LeafFactory factory) {
        return context -> new IpScriptBlockDocValuesReader(factory.newInstance(context));
    }

    private final IpFieldScript script;
    private int docId;

    IpScriptBlockDocValuesReader(IpFieldScript script) {
        this.script = script;
    }

    @Override
    public int docID() {
        return docId;
    }

    @Override
    public BlockLoader.BytesRefBuilder builder(BlockLoader.BuilderFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);  // Note that we don't pre-sort our output so we can't use bytesRefsFromDocValues
    }

    @Override
    public BlockLoader.Block readValues(BlockLoader.BuilderFactory factory, BlockLoader.Docs docs) {
        try (BlockLoader.BytesRefBuilder builder = builder(factory, docs.count())) {
            for (int i = 0; i < docs.count(); i++) {
                read(docs.get(i), builder);
            }
            return builder.build();
        }
    }

    @Override
    public void readValuesFromSingleDoc(int docId, BlockLoader.Builder builder) {
        this.docId = docId;
        read(docId, (BlockLoader.BytesRefBuilder) builder);
    }

    private void read(int docId, BlockLoader.BytesRefBuilder builder) {
        script.runForDoc(docId);
        switch (script.count()) {
            case 0 -> builder.appendNull();
            case 1 -> {
                builder.appendBytesRef(script.values()[0]);
            }
            default -> {
                builder.beginPositionEntry();
                for (int i = 0; i < script.count(); i++) {
                    builder.appendBytesRef(script.values()[i]);
                }
                builder.endPositionEntry();
            }
        }
    }

    @Override
    public String toString() {
        return "ScriptIps";
    }
}
