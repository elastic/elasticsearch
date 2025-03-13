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
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.script.StringFieldScript;

import java.io.IOException;

/**
 * {@link BlockDocValuesReader} implementation for keyword scripts.
 */
public class KeywordScriptBlockDocValuesReader extends BlockDocValuesReader {
    static class KeywordScriptBlockLoader extends DocValuesBlockLoader {
        private final StringFieldScript.LeafFactory factory;

        KeywordScriptBlockLoader(StringFieldScript.LeafFactory factory) {
            this.factory = factory;
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.bytesRefs(expectedCount);
        }

        @Override
        public AllReader reader(LeafReaderContext context) throws IOException {
            return new KeywordScriptBlockDocValuesReader(factory.newInstance(context));
        }
    }

    private final BytesRefBuilder bytesBuild = new BytesRefBuilder();
    private final StringFieldScript script;
    private int docId;

    KeywordScriptBlockDocValuesReader(StringFieldScript script) {
        this.script = script;
    }

    @Override
    public int docId() {
        return docId;
    }

    @Override
    public BlockLoader.Block read(BlockLoader.BlockFactory factory, BlockLoader.Docs docs) throws IOException {
        // Note that we don't pre-sort our output so we can't use bytesRefsFromDocValues
        try (BlockLoader.BytesRefBuilder builder = factory.bytesRefs(docs.count())) {
            for (int i = 0; i < docs.count(); i++) {
                read(docs.get(i), builder);
            }
            return builder.build();
        }
    }

    @Override
    public void read(int docId, BlockLoader.StoredFields storedFields, BlockLoader.Builder builder) throws IOException {
        this.docId = docId;
        read(docId, (BlockLoader.BytesRefBuilder) builder);
    }

    private void read(int docId, BlockLoader.BytesRefBuilder builder) {
        script.runForDoc(docId);
        switch (script.getValues().size()) {
            case 0 -> builder.appendNull();
            case 1 -> {
                bytesBuild.copyChars(script.getValues().get(0));
                builder.appendBytesRef(bytesBuild.get());
            }
            default -> {
                builder.beginPositionEntry();
                for (String v : script.getValues()) {
                    bytesBuild.copyChars(v);
                    builder.appendBytesRef(bytesBuild.get());
                }
                builder.endPositionEntry();
            }
        }
    }

    @Override
    public String toString() {
        return "ScriptKeywords";
    }
}
