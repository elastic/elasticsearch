/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.pattern.PatternTokenizer;

import java.util.regex.Pattern;

/** Simple regex-based analyzer based on PatternTokenizer + lowercase + stopwords */
public final class PatternAnalyzer extends Analyzer {
    private final Pattern pattern;
    private final boolean lowercase;
    private final CharArraySet stopWords;

    PatternAnalyzer(Pattern pattern, boolean lowercase, CharArraySet stopWords) {
        this.pattern = pattern;
        this.lowercase = lowercase;
        this.stopWords = stopWords;
    }

    @Override
    protected TokenStreamComponents createComponents(String s) {
        final Tokenizer tokenizer = new PatternTokenizer(pattern, -1);
        TokenStream stream = tokenizer;
        if (lowercase) {
            stream = new LowerCaseFilter(stream);
        }
        if (stopWords != null) {
            stream = new StopFilter(stream, stopWords);
        }
        return new TokenStreamComponents(tokenizer, stream);
    }

    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
        TokenStream stream = in;
        if (lowercase) {
            stream = new LowerCaseFilter(stream);
        }
        return stream;
    }
}
