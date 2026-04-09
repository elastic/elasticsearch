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

import java.util.Set;

public class KuromojiAnalyzerProvider extends AbstractIndexAnalyzerProvider<JapaneseAnalyzer> {

    private final JapaneseAnalyzer analyzer;

    public KuromojiAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name);
        final Set<?> stopWords = Analysis.parseStopWords(env, settings, JapaneseAnalyzer.getDefaultStopSet());
        final JapaneseTokenizer.Mode mode = KuromojiTokenizerFactory.getMode(settings);
        final UserDictionary userDictionary = KuromojiTokenizerFactory.getUserDictionary(env, settings);
        analyzer = new JapaneseAnalyzer(userDictionary, mode, CharArraySet.copy(stopWords), JapaneseAnalyzer.getDefaultStopTags());
    }

    @Override
    public JapaneseAnalyzer get() {
        return this.analyzer;
    }

}
