/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.CharTokenizer;
import org.elasticsearch.index.analysis.AnalyzerScope;
import org.elasticsearch.index.analysis.NamedAnalyzer;

/**
 * An analyzer that tokenizes text by a pre-defined list of delimiters that work well for log messages.
 * The pre-defined list of delimiters is: whitespace characters, =, ?, :, [, ], {, }, ", \, '
 */
public final class DelimiterAnalyzer extends Analyzer {

    static final NamedAnalyzer INSTANCE = new NamedAnalyzer("delimiter", AnalyzerScope.GLOBAL, new DelimiterAnalyzer());

    private DelimiterAnalyzer() {}

    @Override
    protected TokenStreamComponents createComponents(String s) {
        final Tokenizer tokenizer = new DelimiterTokenizer();
        TokenStream stream = new LowerCaseFilter(tokenizer);
        return new TokenStreamComponents(tokenizer, stream);
    }

    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
        TokenStream stream = in;
        stream = new LowerCaseFilter(stream);
        return stream;
    }

    static final class DelimiterTokenizer extends CharTokenizer {

        DelimiterTokenizer() {
            super(TokenStream.DEFAULT_TOKEN_ATTRIBUTE_FACTORY);
        }

        @Override
        protected boolean isTokenChar(int c) {
            if (Character.isWhitespace(c)
                || c == '='
                || c == '?'
                || c == ':'
                || c == '['
                || c == ']'
                || c == '{'
                || c == '}'
                || c == '"'
                || c == '\\'
                || c == '\'') {
                return false;
            } else {
                return true;
            }
        }
    }
}
