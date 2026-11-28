/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

/**
 * Produces exactly the tokens of a {@link StandardAnalyzer} without stop words, but faster for ASCII text. Log messages, which are
 * nearly always analyzed with the standard analyzer, are almost always ASCII. See {@link AsciiStandardTokenizer} for how the
 * fast path works and when it falls back to Lucene's own tokenizer.
 * <p>
 * Analyzers with stop words must keep using {@link StandardAnalyzer}, this class has no stop filter on purpose.
 */
public final class AsciiStandardAnalyzer extends Analyzer {

    private final int maxTokenLength;

    public AsciiStandardAnalyzer() {
        this(StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH);
    }

    /**
     * @param maxTokenLength same meaning as {@link StandardAnalyzer#setMaxTokenLength}. As there, an invalid value is only
     *                       rejected when the first token stream is created.
     */
    public AsciiStandardAnalyzer(int maxTokenLength) {
        this.maxTokenLength = maxTokenLength;
    }

    public int getMaxTokenLength() {
        return maxTokenLength;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        AsciiStandardTokenizer tokenizer = new AsciiStandardTokenizer();
        tokenizer.setMaxTokenLength(maxTokenLength);
        return new TokenStreamComponents(tokenizer);
    }

    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
        return new LowerCaseFilter(in);
    }
}
