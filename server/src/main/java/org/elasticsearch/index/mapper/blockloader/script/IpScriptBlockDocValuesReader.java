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
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.script.IpFieldScript;

import java.io.IOException;

import static org.elasticsearch.index.mapper.blockloader.script.KeywordScriptBlockDocValuesReader.ESTIMATED_SIZE;

/**
 * {@link BlockDocValuesReader} implementation for keyword scripts.
 */
public class IpScriptBlockDocValuesReader extends BlockDocValuesReader {
    public static class IpScriptBlockLoader extends DocValuesBlockLoader {
        private final IpFieldScript.LeafFactory factory;

        public IpScriptBlockLoader(IpFieldScript.LeafFactory factory) {
            this.factory = factory;
        }

        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.bytesRefs(expectedCount);
        }

        @Override
        public AllReader reader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
            breaker.addEstimateBytesAndMaybeBreak(ESTIMATED_SIZE, "load blocks");
            return new IpScriptBlockDocValuesReader(breaker, factory.newInstance(context));
        }
    }

    private final IpFieldScript script;
    private int docId;

    IpScriptBlockDocValuesReader(CircuitBreaker breaker, IpFieldScript script) {
        super(breaker);
        this.script = script;
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

    @Override
    public void close() {
        breaker.addWithoutBreaking(-ESTIMATED_SIZE);
    }
}
