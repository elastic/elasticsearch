/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
    private final Object sharingKey;

    PatternAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(name);

        final CharArraySet defaultStopwords = CharArraySet.EMPTY_SET;
        boolean lowercase = settings.getAsBoolean("lowercase", true);
        CharArraySet stopWords = Analysis.parseStopWords(env, settings, defaultStopwords);
        // When lowercase=false the stop set matches with its own case-sensitivity, so stopwords_case
        // is behavior-affecting and must be part of the sharing key (see Analysis.StableCharArraySet).
        boolean stopwordsCase = settings.getAsBoolean("stopwords_case", false);

        String sPattern = settings.get("pattern", "\\W+" /*PatternAnalyzer.NON_WORD_PATTERN*/);
        if (sPattern == null) {
            throw new IllegalArgumentException("Analyzer [" + name + "] of type pattern must have a `pattern` set");
        }
        Pattern pattern = Regex.compile(sPattern, settings.get("flags"));

        analyzer = new PatternAnalyzer(pattern, lowercase, stopWords);
        // {@link Pattern} uses identity equality; capture (regex string, flags) for the sharing key.
        this.sharingKey = new Key(pattern.pattern(), pattern.flags(), lowercase, new Analysis.StableCharArraySet(stopWords, stopwordsCase));
    }

    @Override
    public PatternAnalyzer get() {
        return analyzer;
    }

    @Override
    public Object sharingKey() {
        return sharingKey;
    }

    private record Key(String pattern, int flags, boolean lowercase, Analysis.StableCharArraySet stopWords) {}
}
