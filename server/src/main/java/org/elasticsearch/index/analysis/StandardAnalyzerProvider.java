/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

public class StandardAnalyzerProvider extends AbstractIndexAnalyzerProvider<StandardAnalyzer> {

    private final StandardAnalyzer standardAnalyzer;

    public StandardAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name);
        final CharArraySet defaultStopwords = CharArraySet.EMPTY_SET;
        CharArraySet stopWords = Analysis.parseStopWords(env, settings, defaultStopwords);
        int maxTokenLength = settings.getAsInt("max_token_length", StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH);
        standardAnalyzer = new StandardAnalyzer(stopWords);
        standardAnalyzer.setMaxTokenLength(maxTokenLength);
    }

    @Override
    public StandardAnalyzer get() {
        return this.standardAnalyzer;
    }
}
