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

/**
 * Provides a Kuromoji-based Japanese analyzer with configurable tokenization, stop words, and part-of-speech filtering.
 * This analyzer combines Kuromoji tokenization with standard Japanese linguistic processing.
 */
public class KuromojiAnalyzerProvider extends AbstractIndexAnalyzerProvider<JapaneseAnalyzer> {

    private final JapaneseAnalyzer analyzer;

    /**
     * Constructs a Kuromoji analyzer provider with Japanese-specific analysis components.
     *
     * @param indexSettings the index settings
     * @param env the environment for resolving configuration files
     * @param name the analyzer name
     * @param settings the analyzer settings containing:
     *        <ul>
     *        <li>mode: tokenization mode (see {@link KuromojiTokenizerFactory#getMode})</li>
     *        <li>stopwords: custom stop words (defaults to {@link JapaneseAnalyzer#getDefaultStopSet()})</li>
     *        <li>stopwords_path: path to stop words file</li>
     *        <li>user_dictionary: path to user dictionary file</li>
     *        <li>user_dictionary_rules: inline user dictionary rules</li>
     *        </ul>
     * @throws IllegalArgumentException if configuration is invalid
     * @throws ElasticsearchException if user dictionary cannot be loaded
     *
     * <p><b>Usage Example:</b></p>
     * <pre>{@code
     * "analyzer": {
     *   "my_japanese": {
     *     "type": "kuromoji",
     *     "mode": "search",
     *     "stopwords": ["_japanese_", "カスタム"]
     *   }
     * }
     * }</pre>
     */
    public KuromojiAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name);
        final Set<?> stopWords = Analysis.parseStopWords(env, settings, JapaneseAnalyzer.getDefaultStopSet());
        final JapaneseTokenizer.Mode mode = KuromojiTokenizerFactory.getMode(settings);
        final UserDictionary userDictionary = KuromojiTokenizerFactory.getUserDictionary(env, settings);
        analyzer = new JapaneseAnalyzer(userDictionary, mode, CharArraySet.copy(stopWords), JapaneseAnalyzer.getDefaultStopTags());
    }

    /**
     * Returns the configured Japanese analyzer instance.
     *
     * @return the {@link JapaneseAnalyzer} instance
     */
    @Override
    public JapaneseAnalyzer get() {
        return this.analyzer;
    }

}
