/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractIndexAnalyzerProvider;
import org.elasticsearch.index.analysis.Analysis;

import java.util.Map;

/**
 * Creates a SnowballAnalyzer initialized with stopwords and Snowball filter. Only
 * supports Dutch, English (default), French, German and German2 where stopwords
 * are readily available. For other languages available with the Lucene Snowball
 * Stemmer, use them directly with the SnowballFilter and a CustomAnalyzer.
 * Configuration of language is done with the "language" attribute or the analyzer.
 * Also supports additional stopwords via "stopwords" attribute
 * <p>
 * The SnowballAnalyzer comes with a LowerCaseFilter, StopFilter
 * and the SnowballFilter.
 *
 *
 */
public class SnowballAnalyzerProvider extends AbstractIndexAnalyzerProvider<SnowballAnalyzer> {
    private static final Map<String, CharArraySet> DEFAULT_LANGUAGE_STOP_WORDS = Map.of(
        "English",
        EnglishAnalyzer.ENGLISH_STOP_WORDS_SET,
        "Dutch",
        DutchAnalyzer.getDefaultStopSet(),
        "German",
        GermanAnalyzer.getDefaultStopSet(),
        "German2",
        GermanAnalyzer.getDefaultStopSet(),
        "French",
        FrenchAnalyzer.getDefaultStopSet()
    );

    private final SnowballAnalyzer analyzer;

    SnowballAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);

        String language = settings.get("language", settings.get("name", "English"));
        CharArraySet defaultStopwords = DEFAULT_LANGUAGE_STOP_WORDS.getOrDefault(language, CharArraySet.EMPTY_SET);
        CharArraySet stopWords = Analysis.parseStopWords(env, settings, defaultStopwords);

        analyzer = new SnowballAnalyzer(language, stopWords);
    }

    @Override
    public SnowballAnalyzer get() {
        return this.analyzer;
    }
}
