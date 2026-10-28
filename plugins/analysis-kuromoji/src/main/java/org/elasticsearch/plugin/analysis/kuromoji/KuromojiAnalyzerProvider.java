/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.kuromoji;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.ja.JapaneseAnalyzer;
import org.apache.lucene.analysis.ja.JapaneseTokenizer;
import org.apache.lucene.analysis.ja.dict.UserDictionary;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractIndexAnalyzerProvider;
import org.elasticsearch.index.analysis.Analysis;

public class KuromojiAnalyzerProvider extends AbstractIndexAnalyzerProvider<JapaneseAnalyzer> {

    private final JapaneseAnalyzer analyzer;
    private final Object sharingKey;

    public KuromojiAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name);
        final CharArraySet stopWords = Analysis.parseStopWords(env, settings, JapaneseAnalyzer.getDefaultStopSet());
        final JapaneseTokenizer.Mode mode = KuromojiTokenizerFactory.getMode(settings);
        final Analysis.LoadedUserDictionary<UserDictionary> userDictionary = KuromojiTokenizerFactory.loadUserDictionary(env, settings);
        analyzer = new JapaneseAnalyzer(
            userDictionary.dictionary(),
            mode,
            CharArraySet.copy(stopWords),
            JapaneseAnalyzer.getDefaultStopTags()
        );
        this.sharingKey = new Key(userDictionary.rulesSha256(), mode, Analysis.stableStopWords(settings, stopWords));
    }

    @Override
    public JapaneseAnalyzer get() {
        return this.analyzer;
    }

    @Override
    public Object sharingKey() {
        return sharingKey;
    }

    private record Key(String userDictionaryRulesSha256, JapaneseTokenizer.Mode mode, Analysis.StableCharArraySet stopWords) {}
}
