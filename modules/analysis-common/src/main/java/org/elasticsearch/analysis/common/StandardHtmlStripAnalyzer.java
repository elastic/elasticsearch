/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;

public class StandardHtmlStripAnalyzer extends StopwordAnalyzerBase {

    /**
     * @deprecated use {@link StandardHtmlStripAnalyzer#StandardHtmlStripAnalyzer(CharArraySet)} instead
     */
    @Deprecated
    public StandardHtmlStripAnalyzer() {
        super(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
    }

    /**
     * @deprecated in 6.5, can not create in 7.0, and we remove this in 8.0
     */
    @Deprecated
    StandardHtmlStripAnalyzer(CharArraySet stopwords) {
        super(stopwords);
    }

    @Override
    protected TokenStreamComponents createComponents(final String fieldName) {
        final Tokenizer src = new StandardTokenizer();
        TokenStream tok = new LowerCaseFilter(src);
        if (stopwords.isEmpty() == false) {
            tok = new StopFilter(tok, stopwords);
        }
        return new TokenStreamComponents(src, tok);
    }

}
