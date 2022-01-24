/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.lucene;

import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.plugins.analysis.AnalyzeToken;
import org.elasticsearch.plugins.analysis.ReaderProvider;

public class StableLuceneTokenizerIterator extends StableLuceneFilterIterator {
    private final ReaderProvider readerProvider;
    private final Tokenizer tokenizer;

    public StableLuceneTokenizerIterator(Tokenizer stream, ReaderProvider provider) {
        super(stream);

        this.tokenizer = stream;
        this.readerProvider = provider;
    }

    @Override
    public AnalyzeToken reset() {
        try {
            tokenizer.setReader(readerProvider.getReader());
            return super.reset();
        } catch (Throwable t) {
            throw new IllegalArgumentException("Unsupported token stream operation", t);
        }
    }
}
