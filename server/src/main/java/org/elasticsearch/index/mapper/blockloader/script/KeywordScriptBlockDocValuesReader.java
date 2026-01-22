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
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.script.StringFieldScript;

import java.io.IOException;

/**
 * {@link BlockDocValuesReader} implementation for keyword scripts.
 */
public class KeywordScriptBlockDocValuesReader extends BlockDocValuesReader {
    public static class KeywordScriptBlockLoader extends DocValuesBlockLoader {
        private final StringFieldScript.LeafFactory factory;
        private final long byteSize;

        public KeywordScriptBlockLoader(StringFieldScript.LeafFactory factory, ByteSizeValue byteSize) {
            this.factory = factory;
            this.byteSize = byteSize.getBytes();
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.bytesRefs(expectedCount);
        }

        @Override
        public AllReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
            breaker.addEstimateBytesAndMaybeBreak(byteSize, "load blocks");
            StringFieldScript script = null;
            try {
                script = factory.newInstance(context);
                return new KeywordScriptBlockDocValuesReader(breaker, script, byteSize);
            } finally {
                if (script == null) {
                    breaker.addWithoutBreaking(-byteSize);
                }
            }
        }
    }

    private final BytesRefBuilder bytesBuild = new BytesRefBuilder(); // TODO breaking builder
    private final StringFieldScript script;
    private final long byteSize;
    private int docId;

    KeywordScriptBlockDocValuesReader(CircuitBreaker breaker, StringFieldScript script, long byteSize) {
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
        // Note that we don't pre-sort our output so we can't use bytesRefsFromDocValues
        try (BlockLoader.BytesRefBuilder builder = factory.bytesRefs(docs.count() - offset)) {
            for (int i = offset; i < docs.count(); i++) {
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

    @Override
    public void close() {
        breaker.addWithoutBreaking(-byteSize);
    }
}
