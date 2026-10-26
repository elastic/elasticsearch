/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugin.analysis.kuromoji;

import org.apache.lucene.analysis.ja.JapaneseCompletionAnalyzer;
import org.apache.lucene.analysis.ja.JapaneseCompletionFilter.Mode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractIndexAnalyzerProvider;

public class KuromojiCompletionAnalyzerProvider extends AbstractIndexAnalyzerProvider<JapaneseCompletionAnalyzer> {

    private final JapaneseCompletionAnalyzer analyzer;
    private final Object sharingKey;

    public KuromojiCompletionAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name);
        final KuromojiTokenizerFactory.LoadedUserDictionary userDictionary = KuromojiTokenizerFactory.loadUserDictionary(env, settings);
        final Mode mode = KuromojiCompletionFilterFactory.getMode(settings);
        analyzer = new JapaneseCompletionAnalyzer(userDictionary.dictionary(), mode);
        this.sharingKey = new Key(userDictionary.rulesSha256(), mode);
    }

    @Override
    public JapaneseCompletionAnalyzer get() {
        return analyzer;
    }

    @Override
    public Object sharingKey() {
        return sharingKey;
    }

    private record Key(String userDictionaryRulesSha256, Mode mode) {}
}
