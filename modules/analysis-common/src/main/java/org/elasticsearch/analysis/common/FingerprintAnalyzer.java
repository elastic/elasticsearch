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
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.FingerprintFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/** OpenRefine Fingerprinting, which uses a Standard tokenizer and lowercase + stop + fingerprint + asciifolding filters */
public final class FingerprintAnalyzer extends Analyzer {
    private final char separator;
    private final int maxOutputSize;
    private final CharArraySet stopWords;

    FingerprintAnalyzer(CharArraySet stopWords, char separator, int maxOutputSize) {
        this.separator = separator;
        this.maxOutputSize = maxOutputSize;
        this.stopWords = stopWords;
    }

    @Override
    protected TokenStreamComponents createComponents(String s) {
        final Tokenizer tokenizer = new StandardTokenizer();
        TokenStream stream = tokenizer;
        stream = new LowerCaseFilter(stream);
        stream = new ASCIIFoldingFilter(stream, false);
        stream = new StopFilter(stream, stopWords);
        stream = new FingerprintFilter(stream, maxOutputSize, separator);
        return new TokenStreamComponents(tokenizer, stream);
    }
}
