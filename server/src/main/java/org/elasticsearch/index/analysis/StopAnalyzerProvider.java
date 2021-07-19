/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;

public class StopAnalyzerProvider extends AbstractIndexAnalyzerProvider<StopAnalyzer> {

    private final StopAnalyzer stopAnalyzer;

    public StopAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);
        CharArraySet stopWords = Analysis.parseStopWords(
            env, settings, EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
        this.stopAnalyzer = new StopAnalyzer(stopWords);
    }

    @Override
    public StopAnalyzer get() {
        return this.stopAnalyzer;
    }
}
