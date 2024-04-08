/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis.kuromoji;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ja.JapaneseAnalyzer;
import org.apache.lucene.analysis.ja.JapanesePartOfSpeechStopFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.Analysis;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class KuromojiPartOfSpeechFilterFactory extends AbstractTokenFilterFactory {

    private final Set<String> stopTags = new HashSet<>();

    public KuromojiPartOfSpeechFilterFactory(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name, settings);
        List<String> wordList = Analysis.getWordList(env, settings, "stoptags");
        if (wordList != null) {
            stopTags.addAll(wordList);
        } else {
            stopTags.addAll(JapaneseAnalyzer.getDefaultStopTags());
        }
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new JapanesePartOfSpeechStopFilter(tokenStream, stopTags);
    }

}
