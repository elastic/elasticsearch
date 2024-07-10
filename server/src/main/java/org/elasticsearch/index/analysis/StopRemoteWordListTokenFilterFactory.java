/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

import java.util.Set;

public class StopRemoteWordListTokenFilterFactory extends AbstractTokenFilterFactory {

    private final CharArraySet stopWords;

    private final boolean ignoreCase;

    public StopRemoteWordListTokenFilterFactory(
        IndexSettings indexSettings,
        Environment env,
        String name,
        Settings settings,
        WordListsIndexService wordListsIndexService
    ) {
        super(name, settings);
        this.ignoreCase = settings.getAsBoolean("ignore_case", false);
        this.stopWords = Analysis.parseStopWords(env, settings, EnglishAnalyzer.ENGLISH_STOP_WORDS_SET, ignoreCase, wordListsIndexService);
        if (settings.get("enable_position_increments") != null) {
            throw new IllegalArgumentException("enable_position_increments is not supported anymore. Please fix your analysis chain");
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new StopFilter(tokenStream, stopWords);
    }

    public Set<?> stopWords() {
        return stopWords;
    }

    public boolean ignoreCase() {
        return ignoreCase;
    }

}
