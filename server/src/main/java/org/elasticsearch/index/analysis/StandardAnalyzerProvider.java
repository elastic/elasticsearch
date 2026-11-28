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
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

public class StandardAnalyzerProvider extends AbstractIndexAnalyzerProvider<Analyzer> {

    private final Analyzer standardAnalyzer;
    private final Object sharingKey;

    public StandardAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name);
        final CharArraySet defaultStopwords = CharArraySet.EMPTY_SET;
        CharArraySet stopWords = Analysis.parseStopWords(env, settings, defaultStopwords);
        int maxTokenLength = settings.getAsInt("max_token_length", StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH);
        if (stopWords.isEmpty()) {
            // The default recipe, which is what nearly all log analytics uses, has a much faster implementation for ASCII text
            // that produces exactly the same tokens.
            standardAnalyzer = new AsciiStandardAnalyzer(maxTokenLength);
        } else {
            StandardAnalyzer withStopWords = new StandardAnalyzer(stopWords);
            withStopWords.setMaxTokenLength(maxTokenLength);
            standardAnalyzer = withStopWords;
        }
        // stopwords_case is not documented for the standard analyzer but is honoured in practice
        // (parseStopWords reads it and the stop filter respects it for uppercase stopwords), so fold
        // it into the sharing key — two recipes differing only in case handling must not share.
        this.sharingKey = new Key(Analysis.stableStopWords(settings, stopWords), maxTokenLength);
    }

    @Override
    public Analyzer get() {
        return this.standardAnalyzer;
    }

    @Override
    public Object sharingKey() {
        return sharingKey;
    }

    private record Key(Analysis.StableCharArraySet stopWords, int maxTokenLength) {}
}
