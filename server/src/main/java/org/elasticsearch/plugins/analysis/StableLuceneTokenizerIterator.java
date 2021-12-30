/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.analysis;

import java.io.Reader;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

public class StableLuceneTokenizerIterator extends StableLuceneFilterIterator {
    private final ReaderProvider readerProvider;

    private final MethodHandle mhSetReader;

    public StableLuceneTokenizerIterator(Object stream, AnalyzeState prevState, ReaderProvider provider) {
        super(stream, prevState);
        StablePluginAPIUtil.ensureClassCompatibility(stream.getClass(), "org.apache.lucene.analysis.Tokenizer");

        this.readerProvider = provider;

        MethodHandles.Lookup lookup = MethodHandles.lookup();

        try {
            Class<?> tokenizerClass = StablePluginAPIUtil.lookupClass(stream, "org.apache.lucene.analysis.Tokenizer");
            mhSetReader = lookup.findVirtual(
                tokenizerClass, "setReader", MethodType.methodType(void.class, Reader.class));
        } catch (Throwable x) {
            throw new IllegalArgumentException("Incompatible Lucene library provided", x);
        }
    }

    @Override
    public AnalyzeToken reset() {
        try {
            mhSetReader.invoke(stream, readerProvider.getReader());
            return super.reset();
        } catch (Throwable t) {
            throw new IllegalArgumentException("Unsupported token stream operation", t);
        }
    }
}
