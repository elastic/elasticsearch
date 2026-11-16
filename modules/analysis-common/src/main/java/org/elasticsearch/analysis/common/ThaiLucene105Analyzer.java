/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.DecimalDigitFilter;
import org.apache.lucene.util.Version;
import org.elasticsearch.index.analysis.ThaiLucene105StopWords;

/**
 * The Lucene 10.5 {@code ThaiAnalyzer}: {@link ThaiLucene105Tokenizer}, lowercase, decimal digit, stop, with the 10.5 default stopword
 * list. No {@code ThaiCharFilter}, {@code ThaiNormalizationFilter} or {@code ThaiRepeatFilter}. Lucene's {@code ThaiAnalyzer} is final
 * and changed its chain in 10.6, so the old chain is rebuilt here. Used for indices created before
 * {@link Version#LUCENE_10_6_0} so that query-time analysis keeps matching the terms they were indexed with.
 */
final class ThaiLucene105Analyzer extends StopwordAnalyzerBase {

    /**
     * The Lucene 10.5 default stop set. It lives in server ({@link ThaiLucene105StopWords}) so that {@code Analysis} can resolve
     * {@code _thai_} for old indices without a cross-module hook.
     */
    static CharArraySet getDefaultStopSet() {
        return ThaiLucene105StopWords.defaultStopSet();
    }

    ThaiLucene105Analyzer() {
        this(ThaiLucene105StopWords.defaultStopSet());
    }

    ThaiLucene105Analyzer(CharArraySet stopwords) {
        super(stopwords);
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        final Tokenizer source = new ThaiLucene105Tokenizer();
        TokenStream result = new LowerCaseFilter(source);
        result = new DecimalDigitFilter(result);
        result = new StopFilter(result, stopwords);
        return new TokenStreamComponents(source, result);
    }

    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
        TokenStream result = new LowerCaseFilter(in);
        result = new DecimalDigitFilter(result);
        return result;
    }
    // Deliberately no initReader / initReaderForNormalization override: Lucene 10.5 applied no char filter.
}
