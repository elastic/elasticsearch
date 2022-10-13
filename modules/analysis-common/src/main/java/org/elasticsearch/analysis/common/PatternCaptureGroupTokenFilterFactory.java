/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.pattern.PatternCaptureGroupTokenFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;

import java.util.List;
import java.util.regex.Pattern;

public class PatternCaptureGroupTokenFilterFactory extends AbstractTokenFilterFactory {
    private final Pattern[] patterns;
    private final boolean preserveOriginal;
    private static final String PATTERNS_KEY = "patterns";
    private static final String PRESERVE_ORIG_KEY = "preserve_original";

    PatternCaptureGroupTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name, settings);
        List<String> regexes = settings.getAsList(PATTERNS_KEY, null, false);
        if (regexes == null) {
            throw new IllegalArgumentException("required setting '" + PATTERNS_KEY + "' is missing for token filter [" + name + "]");
        }
        patterns = new Pattern[regexes.size()];
        for (int i = 0; i < regexes.size(); i++) {
            patterns[i] = Pattern.compile(regexes.get(i));
        }

        preserveOriginal = settings.getAsBoolean(PRESERVE_ORIG_KEY, true);
    }

    @Override
    public TokenFilter create(TokenStream tokenStream) {
        return new PatternCaptureGroupTokenFilter(tokenStream, preserveOriginal, patterns);
    }
}
