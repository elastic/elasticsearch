/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.elasticsearch.common.util.CollectionUtils;

import java.io.Reader;

public final class CustomAnalyzer extends Analyzer implements AnalyzerComponentsProvider {

    private final AnalyzerComponents components;
    private final int positionIncrementGap;
    private final int offsetGap;
    private final AnalysisMode analysisMode;

    public CustomAnalyzer(TokenizerFactory tokenizerFactory, CharFilterFactory[] charFilters,
            TokenFilterFactory[] tokenFilters) {
        this(tokenizerFactory, charFilters, tokenFilters, 0, -1);
    }

    public CustomAnalyzer(TokenizerFactory tokenizerFactory, CharFilterFactory[] charFilters,
            TokenFilterFactory[] tokenFilters, int positionIncrementGap, int offsetGap) {
        this.components = new AnalyzerComponents(tokenizerFactory, charFilters, tokenFilters);
        this.positionIncrementGap = positionIncrementGap;
        this.offsetGap = offsetGap;
        // merge and transfer token filter analysis modes with analyzer
        AnalysisMode mode = AnalysisMode.ALL;
        for (TokenFilterFactory f : tokenFilters) {
            mode = mode.merge(f.getAnalysisMode());
        }
        this.analysisMode = mode;
    }

    public TokenizerFactory tokenizerFactory() {
        return this.components.getTokenizerFactory();
    }

    public TokenFilterFactory[] tokenFilters() {
        return this.components.getTokenFilters();
    }

    public CharFilterFactory[] charFilters() {
        return this.components.getCharFilters();
    }

    @Override
    public int getPositionIncrementGap(String fieldName) {
        return this.positionIncrementGap;
    }

    @Override
    public int getOffsetGap(String field) {
        if (offsetGap < 0) {
            return super.getOffsetGap(field);
        }
        return this.offsetGap;
    }

    public AnalysisMode getAnalysisMode() {
        return this.analysisMode;
    }

    @Override
    public AnalyzerComponents getComponents() {
        return this.components;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = this.tokenizerFactory().create();
        TokenStream tokenStream = tokenizer;
        for (TokenFilterFactory tokenFilter : tokenFilters()) {
            tokenStream = tokenFilter.create(tokenStream);
        }
        return new TokenStreamComponents(tokenizer, tokenStream);
    }

    @Override
    protected Reader initReader(String fieldName, Reader reader) {
        CharFilterFactory[] charFilters = charFilters();
        if (CollectionUtils.isEmpty(charFilters) == false) {
            for (CharFilterFactory charFilter : charFilters) {
                reader = charFilter.create(reader);
            }
        }
        return reader;
    }

    @Override
    protected Reader initReaderForNormalization(String fieldName, Reader reader) {
        for (CharFilterFactory charFilter : charFilters()) {
            reader = charFilter.normalize(reader);
        }
        return reader;
    }

    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
        TokenStream result = in;
        for (TokenFilterFactory filter : tokenFilters()) {
            result = filter.normalize(result);
        }
        return result;
    }
}
