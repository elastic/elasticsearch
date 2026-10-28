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
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.analysis.LegacyThaiStopWords;

/**
 * The Lucene 10.5 {@code ThaiAnalyzer}: {@link LegacyThaiTokenizer}, lowercase, decimal digit, stop, with the 10.5 default stopword
 * list. No {@code ThaiCharFilter}, {@code ThaiNormalizationFilter} or {@code ThaiRepeatFilter}. Lucene's {@code ThaiAnalyzer} is final
 * and changed its chain in 10.6, so the old chain is rebuilt here. Used for indices created before
 * {@link IndexVersions#THAI_ANALYSIS_LUCENE_10_6} so that query-time analysis keeps matching the terms they were indexed with.
 */
final class LegacyThaiAnalyzer extends StopwordAnalyzerBase {

    /**
     * The Lucene 10.5 default stop set. It lives in server ({@link LegacyThaiStopWords}) so that {@code Analysis} can resolve
     * {@code _thai_} for old indices without a cross-module hook.
     */
    static CharArraySet getDefaultStopSet() {
        return LegacyThaiStopWords.DEFAULT_STOP_SET;
    }

    LegacyThaiAnalyzer() {
        this(LegacyThaiStopWords.DEFAULT_STOP_SET);
    }

    LegacyThaiAnalyzer(CharArraySet stopwords) {
        super(stopwords);
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        final Tokenizer source = new LegacyThaiTokenizer();
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
