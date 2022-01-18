/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractIndexAnalyzerProvider;
import org.elasticsearch.index.analysis.Analysis;

import java.util.regex.Pattern;

public class PatternAnalyzerProvider extends AbstractIndexAnalyzerProvider<Analyzer> {

    private final PatternAnalyzer analyzer;

    PatternAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);

        final CharArraySet defaultStopwords = CharArraySet.EMPTY_SET;
        boolean lowercase = settings.getAsBoolean("lowercase", true);
        CharArraySet stopWords = Analysis.parseStopWords(env, settings, defaultStopwords);

        String sPattern = settings.get("pattern", "\\W+" /*PatternAnalyzer.NON_WORD_PATTERN*/);
        if (sPattern == null) {
            throw new IllegalArgumentException("Analyzer [" + name + "] of type pattern must have a `pattern` set");
        }
        Pattern pattern = Regex.compile(sPattern, settings.get("flags"));

        analyzer = new PatternAnalyzer(pattern, lowercase, stopWords);
    }

    @Override
    public PatternAnalyzer get() {
        return analyzer;
    }
}
