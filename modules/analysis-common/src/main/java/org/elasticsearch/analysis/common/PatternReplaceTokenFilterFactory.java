/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.common;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.pattern.PatternReplaceFilter;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AbstractTokenFilterFactory;
import org.elasticsearch.index.analysis.NormalizingTokenFilterFactory;

import java.util.regex.Pattern;

public class PatternReplaceTokenFilterFactory extends AbstractTokenFilterFactory implements NormalizingTokenFilterFactory {

    private final Pattern pattern;
    private final String replacement;
    private final boolean all;

    public PatternReplaceTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(name, settings);

        String sPattern = settings.get("pattern", null);
        if (sPattern == null) {
            throw new IllegalArgumentException("pattern is missing for [" + name + "] token filter of type 'pattern_replace'");
        }
        this.pattern = Regex.compile(sPattern, settings.get("flags"));
        this.replacement = settings.get("replacement", "");
        this.all = settings.getAsBoolean("all", true);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        return new PatternReplaceFilter(tokenStream, pattern, replacement, all);
    }
}
