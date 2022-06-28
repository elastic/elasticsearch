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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractIndexAnalyzerProvider;
import org.elasticsearch.index.analysis.Analysis;
import org.elasticsearch.xcontent.ParseField;

/**
 * Builds an OpenRefine Fingerprint analyzer.  Uses the default settings from the various components
 * (Standard Tokenizer and lowercase + stop + fingerprint + ascii-folding filters)
 */
public class FingerprintAnalyzerProvider extends AbstractIndexAnalyzerProvider<Analyzer> {

    public static ParseField SEPARATOR = new ParseField("separator");
    public static ParseField MAX_OUTPUT_SIZE = new ParseField("max_output_size");

    public static int DEFAULT_MAX_OUTPUT_SIZE = 255;
    public static CharArraySet DEFAULT_STOP_WORDS = CharArraySet.EMPTY_SET;
    public static final char DEFAULT_SEPARATOR = ' ';

    private final FingerprintAnalyzer analyzer;

    FingerprintAnalyzerProvider(IndexSettings indexSettings, Environment env, String name, Settings settings) {
        super(indexSettings, name, settings);

        char separator = parseSeparator(settings);
        int maxOutputSize = settings.getAsInt(MAX_OUTPUT_SIZE.getPreferredName(), DEFAULT_MAX_OUTPUT_SIZE);
        CharArraySet stopWords = Analysis.parseStopWords(env, settings, DEFAULT_STOP_WORDS);

        this.analyzer = new FingerprintAnalyzer(stopWords, separator, maxOutputSize);
    }

    @Override
    public FingerprintAnalyzer get() {
        return analyzer;
    }

    public static char parseSeparator(Settings settings) throws IllegalArgumentException {
        String customSeparator = settings.get(SEPARATOR.getPreferredName());
        if (customSeparator == null) {
            return DEFAULT_SEPARATOR;
        } else if (customSeparator.length() == 1) {
            return customSeparator.charAt(0);
        }

        throw new IllegalArgumentException(
            "Setting [separator] must be a single, non-null character. [" + customSeparator + "] was provided."
        );
    }
}
