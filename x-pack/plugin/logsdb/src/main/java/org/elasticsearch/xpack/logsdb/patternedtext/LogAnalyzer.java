/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.pattern.PatternTokenizer;
import org.elasticsearch.common.regex.Regex;

import java.util.regex.Pattern;

public final class LogAnalyzer extends Analyzer {

    public static final LogAnalyzer INSTANCE = new LogAnalyzer();

    private final Pattern pattern;

    private LogAnalyzer() {
        this.pattern = Regex.compile("[\\s\\=\\?\\:\\[\\]\\{\\}\\\"\\\\\\']", null);
    }

    @Override
    protected TokenStreamComponents createComponents(String s) {
        // TODO: split tokens on =,?:, that are not dates or urls
        final Tokenizer tokenizer = new PatternTokenizer(pattern, -1);
        TokenStream stream = new LowerCaseFilter(tokenizer);
        return new TokenStreamComponents(tokenizer, stream);
    }

    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
        TokenStream stream = in;
        stream = new LowerCaseFilter(stream);
        return stream;
    }
}
